package cc.datafabric.pipelines.io;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class AccumuloSingleTableWrite
        extends PTransform<PCollection<Iterable<Mutation>>, PCollection<Boolean>> {

    private final String instanceName;
    private final String zookeepers;
    private final String username;
    private final String password;
    private final String tableName;

    public AccumuloSingleTableWrite(String instanceName, String zookeeperServers, String username, String password,
                                    String tableName) {
        this.instanceName = instanceName;
        this.zookeepers = zookeeperServers;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
    }

    @Override
    public PCollection<Boolean> expand(PCollection<Iterable<Mutation>> input) {
        return input.apply(ParDo.of(new WriterDoFn()));
    }

    private class WriterDoFn extends DoFn<Iterable<Mutation>, Boolean> {

        private transient Connector connector;
        private transient BatchWriter writer;

        @Setup
        public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
            final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);

            this.connector = instance.getConnector(username, new PasswordToken(password));

            BatchWriterConfig writerConfig = new BatchWriterConfig();

            this.writer = this.connector.createBatchWriter(tableName, writerConfig);
        }

        @ProcessElement
        public void processElement(@Element Iterable<Mutation> mutations, OutputReceiver<Boolean> out)
                throws AccumuloException {
            this.writer.addMutations(mutations);

            this.writer.flush();

            out.output(true);
        }

        @Teardown
        public void tearDown() throws MutationsRejectedException {
            this.writer.flush();
            this.writer.close();
        }

    }
}
