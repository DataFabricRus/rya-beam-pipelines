package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.coders.MapEntryCoder;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AccumuloReadAll
        extends PTransform<PCollection<String>, PCollection<Map.Entry<Key, Value>>>
        implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AccumuloReadAll.class);

    private final String instanceName;
    private final String zookeeperServers;
    private final String username;
    private final String password;
    private int iteratorPriority;
    private final Class<? extends SortedKeyValueIterator<Key, Value>> iteratorClass;
    private final Map<String, String> iteratorOptions;

    AccumuloReadAll(String instanceName, String zookeeperServers, String username, String password,
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
    public PCollection<Map.Entry<Key, Value>> expand(PCollection<String> input) {
        return input
                .apply(new AccumuloFetchSplits(instanceName, zookeeperServers, username, password))
                .apply(Reshuffle.viaRandomKey())
                .apply(ParDo.of(new DoFn<KV<String, Range>, Map.Entry<Key, Value>>() {

                    @ProcessElement
                    public void processElement(@Element KV<String, Range> element,
                                               OutputReceiver<Map.Entry<Key, Value>> receiver)
                            throws Exception {
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

                            scanner.forEach(receiver::output);
                        }

                        LOG.info("Finished reading a range [{} -> {}]",
                                element.getValue().getStartKey(), element.getValue().getEndKey());
                    }
                }))
                .setCoder(MapEntryCoder.of(WritableCoder.of(Key.class), WritableCoder.of(Value.class)));
    }
}
