package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.RyaSchemaUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.rya.accumulo.iterators.RDFPropertyFilter;
import org.apache.rya.accumulo.utils.RangeUtils;
import org.apache.rya.api.domain.RyaIRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AccumuloFilterRangesByTimestamp extends PTransform<PCollection<KV<String, Range>>, PCollection<KV<String, Range>>> {

    public static final String KEY_START_TIMESTAMP = Property.MASTER_PREFIX + ".custom.fulltext.timestamp";

    private static final Logger LOG = LoggerFactory.getLogger(AccumuloFilterRangesByTimestamp.class);

    private final String instanceName;
    private final String zookeeperServers;
    private final String username;
    private final String password;
    private final String[] properties;
    private Long startTimestamp = null; // in GWT timezone
    private Long endTimestamp = null; // in GMT timezone

    public AccumuloFilterRangesByTimestamp(
            String instanceName, String zookeeperServers, String username, String password,
            String startDateTime, String endDateTime, String[] properties
    ) {
        this.instanceName = instanceName;
        this.zookeeperServers = zookeeperServers;
        this.username = username;
        this.password = password;

        if (startDateTime != null) {
            ZonedDateTime date = ZonedDateTime.parse(startDateTime, DateTimeFormatter.ISO_DATE_TIME);

            this.startTimestamp = date.toEpochSecond() * 1000;
        }

        if (endDateTime != null) {
            ZonedDateTime date = ZonedDateTime.parse(endDateTime, DateTimeFormatter.ISO_DATE_TIME);

            this.endTimestamp = date.toEpochSecond() * 1000;
        } else {
            throw new IllegalArgumentException("The end date time must be set!");
        }

        this.properties = properties;
    }


    @Override
    public PCollection<KV<String, Range>> expand(PCollection<KV<String, Range>> input) {
        return input.apply(ParDo.of(new TimestampFilterDoFn()));
    }

    private class TimestampFilterDoFn extends DoFn<KV<String, Range>, KV<String, Range>> {

        @ProcessElement
        public void processElement(@Element KV<String, Range> element, OutputReceiver<KV<String, Range>> receiver)
                throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
            final Instance instance = new ZooKeeperInstance(instanceName, zookeeperServers);
            final Connector connector = instance.getConnector(username, new PasswordToken(password));

            if (startTimestamp == null) {
                startTimestamp = fetchStartDateTimeFromZookeeper(connector);
            }

            try (Scanner scanner = connector.createScanner(element.getKey(), Authorizations.EMPTY)) {
                scanner.setRange(element.getValue());

                IteratorSetting rpfSettings = new IteratorSetting(1000, RDFPropertyFilter.class);
                rpfSettings.addOption(RDFPropertyFilter.OPTION_PROPERTIES, String.join(",", properties));

                scanner.addScanIterator(rpfSettings);

                IteratorSetting tfSettings = new IteratorSetting(1001, TimestampFilter.class);
                if (startTimestamp != null) {
                    tfSettings.addOption(TimestampFilter.START, "LONG" + startTimestamp);
                }
                tfSettings.addOption(TimestampFilter.END, "LONG" + endTimestamp);

                scanner.addScanIterator(tfSettings);

                LOG.info("Filter rows by timestamp within [{} : {}] range", startTimestamp, endTimestamp);

                scanner.forEach(entry -> {
                    String subject = RyaSchemaUtils.extractSubject(entry.getKey());

                    Range range = RangeUtils.createRange(new RyaIRI(subject));

                    receiver.output(KV.of(element.getKey(), range));
                });
            }
        }

        private Long fetchStartDateTimeFromZookeeper(Connector connector)
                throws AccumuloSecurityException, AccumuloException {
            Map<String, String> conf = connector.instanceOperations().getSystemConfiguration();
            if (conf.containsKey(KEY_START_TIMESTAMP)) {
                return Long.parseUnsignedLong(conf.get(KEY_START_TIMESTAMP));
            }

            return null;
        }

    }
}
