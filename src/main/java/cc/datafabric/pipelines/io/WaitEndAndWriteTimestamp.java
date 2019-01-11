package cc.datafabric.pipelines.io;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class WaitEndAndWriteTimestamp extends PTransform<PCollection<Boolean>, PDone> {

    private final String instanceName;
    private final String zookeeperServers;
    private final String username;
    private final String password;
    private final String endDateTime;

    public WaitEndAndWriteTimestamp(
            String instanceName, String zookeeperServers, String username, String password, String endDateTime
    ) {
        this.instanceName = instanceName;
        this.zookeeperServers = zookeeperServers;
        this.username = username;
        this.password = password;
        this.endDateTime = endDateTime;
    }

    @Override
    public PDone expand(PCollection<Boolean> input) {
        input
                .apply(ParDo.of(new DoFn<Boolean, KV<String, Boolean>>() {

                    @ProcessElement
                    public void processElement(@Element Boolean element, OutputReceiver<KV<String, Boolean>> receiver) {
                        receiver.output(KV.of("", element));
                    }

                }))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new WriteEndDateTime()));

        return PDone.in(input.getPipeline());
    }

    private class WriteEndDateTime extends DoFn<KV<String, Iterable<Boolean>>, Boolean> {

        @ProcessElement
        public void processElement() throws AccumuloSecurityException, AccumuloException {
            final Instance instance = new ZooKeeperInstance(instanceName, zookeeperServers);
            final Connector connector = instance.getConnector(username, new PasswordToken(password));

            if(endDateTime != null) {
                long timestamp = ZonedDateTime.parse(endDateTime, DateTimeFormatter.ISO_DATE_TIME)
                        .toEpochSecond() * 1000;
                connector.instanceOperations()
                        .setProperty(AccumuloFilterRangesByTimestamp.KEY_START_TIMESTAMP, String.valueOf(timestamp));
            }
        }

    }
}
