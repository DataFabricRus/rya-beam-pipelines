package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.RyaSchemaUtils;
import cc.datafabric.pipelines.coders.MapEntryCoder;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;

public class AccumuloReadAndGroupByRyaSubject
        extends PTransform<PCollection<KV<String, Range>>, PCollection<KV<String, Iterable<Map.Entry<Key, Value>>>>>
        implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AccumuloReadAndGroupByRyaSubject.class);

    private final String instanceName;
    private final String zookeeperServers;
    private final String username;
    private final String password;
    private int iteratorPriority;
    private final Class<? extends SortedKeyValueIterator<Key, Value>> iteratorClass;
    private final Map<String, String> iteratorOptions;

    public AccumuloReadAndGroupByRyaSubject(String instanceName, String zookeeperServers, String username, String password,
                                            int iteratorPriority, Class<? extends SortedKeyValueIterator<Key, Value>> iteratorClass,
                                            Map<String, String> iteratorOptions) {
        this.instanceName = instanceName;
        this.zookeeperServers = zookeeperServers;
        this.username = username;
        this.password = password;
        this.iteratorPriority = iteratorPriority;
        this.iteratorClass = iteratorClass;
        this.iteratorOptions = iteratorOptions;
    }

    @Override
    public PCollection<KV<String, Iterable<Map.Entry<Key, Value>>>> expand(PCollection<KV<String, Range>> input) {
        return input
                .apply(ParDo.of(new ReadAndGroupBySubjectDoFn()))
                .setCoder(KvCoder.of(
                        StringUtf8Coder.of(),
                        IterableCoder.of(MapEntryCoder.of(WritableCoder.of(Key.class), WritableCoder.of(Value.class)))
                ));
    }

    private class ReadAndGroupBySubjectDoFn extends DoFn<KV<String, Range>, KV<String, Iterable<Map.Entry<Key, Value>>>> {

        @ProcessElement
        public void processElement(
                @Element KV<String, Range> element, OutputReceiver<KV<String, Iterable<Map.Entry<Key, Value>>>> receiver
        ) throws Exception {
            final Instance instance = new ZooKeeperInstance(instanceName, zookeeperServers);
            final Connector connector = instance.getConnector(username, new PasswordToken(password));

            try (Scanner scanner = connector.createScanner(element.getKey(), Authorizations.EMPTY)) {
                LOG.info("Started reading a range [{} -> {}]",
                        element.getValue().getStartKey(), element.getValue().getEndKey());
                scanner.setRange(element.getValue());

                if (iteratorClass != null) {
                    if (iteratorOptions != null) {
                        scanner.addScanIterator(new IteratorSetting(
                                iteratorPriority, iteratorClass, iteratorOptions));
                    } else {
                        scanner.addScanIterator(new IteratorSetting(iteratorPriority, iteratorClass));
                    }
                }

                String previousSubject = null;
                List<Map.Entry<Key, Value>> buffer = new ArrayList<>();
                for (Map.Entry<Key, Value> entry : scanner) {
                    final String currentSubject = RyaSchemaUtils.extractSubject(entry.getKey());

                    if (currentSubject != null) {
                        if (previousSubject == null) {
                            previousSubject = currentSubject;

                            buffer.add(entry);
                        } else if (previousSubject.equals(currentSubject)) {
                            buffer.add(entry);
                        } else {
                            receiver.output(KV.of(previousSubject, buffer));

                            buffer = new ArrayList<>();
                            buffer.add(entry);
                            previousSubject = currentSubject;
                        }
                    }
                }

                if (previousSubject != null && !buffer.isEmpty()) {
                    //Output the last buffer
                    receiver.output(KV.of(previousSubject, buffer));
                }
            }

            LOG.info("Finished reading a range [{} -> {}]",
                    element.getValue().getStartKey(), element.getValue().getEndKey());
        }

    }
}
