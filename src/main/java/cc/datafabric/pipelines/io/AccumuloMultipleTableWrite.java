package cc.datafabric.pipelines.io;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class AccumuloMultipleTableWrite
        extends PTransform<PCollection<KV<String, Iterable<Mutation>>>, PCollection<Boolean>> {

    private final String instanceName;
    private final String zookeepers;
    private final String username;
    private final String password;

    public AccumuloMultipleTableWrite(String instanceName, String zookeeperServers, String username, String password) {
        this.instanceName = instanceName;
        this.zookeepers = zookeeperServers;
        this.username = username;
        this.password = password;
    }

    @Override
    public PCollection<Boolean> expand(PCollection<KV<String, Iterable<Mutation>>> input) {
        return input.apply(ParDo.of(new WriterDoFn()));
    }

    private class WriterDoFn extends DoFn<KV<String, Iterable<Mutation>>, Boolean> {

        private transient Connector connector;
        private transient MultiTableBatchWriter writer;

        @Setup
        public void setup() throws AccumuloSecurityException, AccumuloException {
            final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);

            this.connector = instance.getConnector(username, new PasswordToken(password));
            this.writer = this.connector.createMultiTableBatchWriter(new BatchWriterConfig());
        }

        @ProcessElement
        public void processElement(ProcessContext ctx)
                throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
            String tableName = ctx.element().getKey();
            Iterable<Mutation> mutations = ctx.element().getValue();

            BatchWriter tableWriter = this.writer.getBatchWriter(tableName);

            tableWriter.addMutations(mutations);

            this.writer.flush();

            ctx.output(true);
        }

        @Teardown
        public void tearDown() throws MutationsRejectedException {
            this.writer.flush();
            this.writer.close();
        }

    }
}
