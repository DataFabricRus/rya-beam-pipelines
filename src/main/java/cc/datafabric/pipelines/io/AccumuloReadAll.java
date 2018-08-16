package cc.datafabric.pipelines.io;

import cc.datafabric.pipelines.coders.MapEntryCoder;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AccumuloReadAll extends PTransform<PCollection<String>, PCollection<Map.Entry<Key, Value>>> {

    private static final Logger LOG = LoggerFactory.getLogger(AccumuloReadAll.class);

    private final String instanceName;
    private final String zookeeperServers;
    private final String username;
    private final String password;

    AccumuloReadAll(String instanceName, String zookeeperServers, String username, String password) {
        this.instanceName = instanceName;
        this.zookeeperServers = zookeeperServers;
        this.username = username;
        this.password = password;
    }

    @Override
    public PCollection<Map.Entry<Key, Value>> expand(PCollection<String> input) {
        return input
                .apply(ParDo.of(new DoFn<String, KV<String, Range>>() {
                    @ProcessElement
                    public void processElement(@Element String tableName, OutputReceiver<KV<String, Range>> receiver)
                            throws Exception {
                        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperServers);
                        final Connector connector = instance.getConnector(username, new PasswordToken(password));

                        List<Text> splits = Arrays.asList(connector.tableOperations()
                                .listSplits(tableName)
                                .toArray(new Text[0]));

                        if (splits.isEmpty()) {
                            // Then we're going to read the whole table at once
                            LOG.info("Table {}. There is no splits!", tableName);

                            receiver.output(KV.of(tableName, new Range()));
                        } else {
                            LOG.info("Table {}. Found {} splits", tableName, splits.size());
                            int index = 0;
                            while (index < splits.size()) {
                                Range range;
                                if (index == 0) {
                                    range = new Range(null, false,
                                            splits.get(index), false);
                                } else {
                                    range = new Range(splits.get(index - 1), true,
                                            splits.get(index), false);
                                }

                                receiver.output(KV.of(tableName, range));

                                index++;
                            }

                            receiver.output(KV.of(tableName, new Range(splits.get(index - 1), true,
                                    null, false)));
                        }

                    }
                }))
                .apply(Reshuffle.viaRandomKey())
                .apply(ParDo.of(new DoFn<KV<String, Range>, Map.Entry<Key, Value>>() {

                    @ProcessElement
                    public void processElement(@Element KV<String, Range> element,
                                               OutputReceiver<Map.Entry<Key, Value>> receiver)
                            throws Exception {
                        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperServers);
                        final Connector connector = instance.getConnector(username, new PasswordToken(password));

                        try (Scanner scanner = connector.createScanner(element.getKey(), Authorizations.EMPTY)) {
                            scanner.setRange(element.getValue());

                            scanner.forEach(receiver::output);
                        }
                    }
                }))
                .setCoder(MapEntryCoder.of(WritableCoder.of(Key.class), WritableCoder.of(Value.class)));
    }
}
