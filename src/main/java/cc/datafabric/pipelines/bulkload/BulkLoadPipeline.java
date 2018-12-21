package cc.datafabric.pipelines.bulkload;

import cc.datafabric.pipelines.GroupIntoLocalBatches;
import cc.datafabric.pipelines.coders.MutationCoder;
import cc.datafabric.pipelines.coders.RDF4JModelCoder;
import cc.datafabric.pipelines.coders.RDFFormatCoder;
import cc.datafabric.pipelines.io.AccumuloSingleTableWrite;
import cc.datafabric.pipelines.io.RDF4JIO;
import cc.datafabric.pipelines.options.DefaultRyaPipelineOptions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.rya.accumulo.AccumuloRdfUtils;
import org.apache.rya.accumulo.RyaTableMutationsFactory;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

interface BulkLoadPipelineOptions extends DefaultRyaPipelineOptions {

    String getSource();

    void setSource(String source);

    String getDestinationTablePrefix();

    void setDestinationTablePrefix(String tablePrefix);

    long getBatchSize();

    void setBatchSize(long batchSize);

}

public class BulkLoadPipeline {

    private static final TupleTag<Mutation> SPO_MUTATIONS = new TupleTag<>();
    private static final TupleTag<Mutation> PO_MUTATIONS = new TupleTag<>();
    private static final TupleTag<Mutation> OSP_MUTATIONS = new TupleTag<>();

    static Pipeline create(BulkLoadPipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        p.getCoderRegistry().registerCoderForClass(RDFFormat.class, RDFFormatCoder.of());
        p.getCoderRegistry().registerCoderForClass(Model.class, RDF4JModelCoder.of());

        PCollectionTuple mutations = p
                .apply(Create.of(options.getSource()))
                .apply(new CreateTablesIfNotExist(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getDestinationTablePrefix()
                ))
                .apply(new RDF4JIO.Read(options.getBatchSize()))
                .apply(ParDo.of(new ModelToMutations())
                        .withOutputTags(SPO_MUTATIONS, TupleTagList.of(Arrays.asList(PO_MUTATIONS, OSP_MUTATIONS)))
                );

        mutations.get(SPO_MUTATIONS)
                .setCoder(MutationCoder.of(Mutation.class))
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply(new AccumuloSingleTableWrite(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getDestinationTablePrefix() + "spo"
                ));
        mutations.get(PO_MUTATIONS)
                .setCoder(MutationCoder.of(Mutation.class))
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply(new AccumuloSingleTableWrite(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getDestinationTablePrefix() + "po"
                ));
        mutations.get(OSP_MUTATIONS)
                .setCoder(MutationCoder.of(Mutation.class))
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply(new AccumuloSingleTableWrite(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getDestinationTablePrefix() + "osp"
                ));

        return p;
    }

    private static class CreateTablesIfNotExist extends PTransform<PCollection<String>, PCollection<String>> {

        private final String instanceName;
        private final String zookeeperServers;
        private final String username;
        private final String password;
        private final String tablePrefix;

        CreateTablesIfNotExist(String instanceName, String zookeeperServers, String username, String password,
                               String tablePrefix) {
            this.instanceName = instanceName;
            this.zookeeperServers = zookeeperServers;
            this.username = username;
            this.password = password;
            this.tablePrefix = tablePrefix;
        }

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new DoFn<String, String>() {

                @ProcessElement
                public void processElement(@Element String input, OutputReceiver<String> out)
                        throws AccumuloSecurityException, AccumuloException, TableExistsException {
                    Instance instance = new ZooKeeperInstance(instanceName, zookeeperServers);
                    Connector connector = instance.getConnector(username, new PasswordToken(password));
                    final TableOperations tableOperations = connector.tableOperations();

                    AccumuloRdfUtils.createTableIfNotExist(tableOperations, tablePrefix + "spo");
                    AccumuloRdfUtils.createTableIfNotExist(tableOperations, tablePrefix + "po");
                    AccumuloRdfUtils.createTableIfNotExist(tableOperations, tablePrefix + "osp");
                    AccumuloRdfUtils.createTableIfNotExist(tableOperations, tablePrefix + "ns");

                    out.output(input);
                }

            }));
        }
    }

    private static class ModelToMutations extends DoFn<Model, Mutation> {

        @ProcessElement
        public void processElement(@Element Model model, MultiOutputReceiver mor) throws IOException {
            for (Statement stmt : model) {
                RyaTableMutationsFactory rtc = new RyaTableMutationsFactory(
                        new RyaTripleContext(false));

                Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> indexes = rtc
                        .serialize(RdfToRyaConversions.convertStatement(stmt));

                indexes.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO)
                        .forEach(it -> mor.get(SPO_MUTATIONS).output(it));
                indexes.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO)
                        .forEach(it -> mor.get(PO_MUTATIONS).output(it));
                indexes.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP)
                        .forEach(it -> mor.get(OSP_MUTATIONS).output(it));
            }
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        BulkLoadPipelineOptions options = PipelineOptionsFactory.as(BulkLoadPipelineOptions.class);

        options.setJobName("rya-bulkload");
        options.setProject("core-datafabric");
        options.setRegion("europe-west1");
        options.setTempLocation("gs://datafabric-dataflow/temp");
        options.setGcpTempLocation("gs://datafabric-dataflow/staging");
        options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.dataflow.DataflowRunner"));
//        options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.direct.DirectRunner"));
        options.setMaxNumWorkers(40);

        options.setAccumuloName("accumulo");
        options.setZookeeperServers("10.132.0.18:2181");
        options.setAccumuloUsername("root");
        options.setAccumuloPassword("accumulo");

        options.setBatchSize(500000);

        options.setSource("gs://fibo-rdf/addresses/*.nt,gs://fibo-rdf/fibo-ru-activities/,gs://fibo-rdf/fibo-ru/," +
                "gs://fibo-rdf/foreignle/*.nt,gs://fibo-rdf/gov/*.nt,gs://fibo-rdf/individuals/*.nt," +
                "gs://fibo-rdf/le/*.nt,gs://fibo-rdf/people/*.nt,gs://fibo-rdf/pif/*.nt,gs://fibo-rdf/rosstat-2012/*.nt," +
                "gs://fibo-rdf/rosstat-2013/*.nt,gs://fibo-rdf/rosstat-2014/*.nt,gs://fibo-rdf/rosstat-2015/*.nt," +
                "gs://fibo-rdf/rosstat-2016/*.nt,gs://fibo-rdf/rosstat/,gs://fibo-rdf/ui/");
        options.setDestinationTablePrefix("triplestore_");

        Pipeline p = BulkLoadPipeline.create(options);
        p.run();
    }

}
