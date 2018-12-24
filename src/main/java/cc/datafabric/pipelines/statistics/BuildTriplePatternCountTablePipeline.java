package cc.datafabric.pipelines.statistics;

import cc.datafabric.pipelines.AvroContainer;
import cc.datafabric.pipelines.GroupIntoLocalBatches;
import cc.datafabric.pipelines.coders.IntermediateProspectCoder;
import cc.datafabric.pipelines.coders.MapEntryCoder;
import cc.datafabric.pipelines.coders.MutationCoder;
import cc.datafabric.pipelines.coders.RangeCoder;
import cc.datafabric.pipelines.io.AccumuloIO;
import cc.datafabric.pipelines.io.AccumuloSingleTableWrite;
import cc.datafabric.pipelines.options.DefaultRyaPipelineOptions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.domain.TripleValueType;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.utils.ProspectorConstants;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.rya.prospector.utils.ProspectorConstants.COUNT;

interface BuildTriplePatternCountTablePipelineOptions extends DefaultRyaPipelineOptions {

    String getSource();

    void setSource(String source);

    String getDestination();

    void setDestination(String destination);

    long getBatchSize();

    void setBatchSize(long batchSize);

}

/**
 * Set of pipelines which help to build the Prospect table.
 *
 * <ul>
 * <li>{@link #create(BuildTriplePatternCountTablePipelineOptions)} - pipeline which does all the steps,</li>
 * <li>{@link #createFetchOnly(BuildTriplePatternCountTablePipelineOptions)} - pipeline which reads the index and save prospects to files,</li>
 * <li>{@link #createCombinerAndWriter(BuildTriplePatternCountTablePipelineOptions)} - pipeline which reads prospects from the files, aggregate them and writes to a table.</li>
 * </ul>
 */
class BuildTriplePatternCountTablePipeline {

    private static final Logger LOG = LoggerFactory.getLogger(BuildTriplePatternCountTablePipeline.class);

    public static Pipeline create(BuildTriplePatternCountTablePipelineOptions options) {
        Pipeline p = preparePipeline(options);

        p
                .apply(Create.of(options.getSource()))
                .apply("Prepare tables", new PrepareDestinationTable<>())
                .apply(new AccumuloIO.Read(
                        options.getAccumuloName(), options.getZookeeperServers(),
                        options.getAccumuloUsername(), options.getAccumuloPassword()
                ))
                .apply(new TripleToProspects())
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply(new AggregateProspects())
                .apply(new ProspectToMutation())
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply("Write Mutations", new AccumuloSingleTableWrite(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getDestination()
                ));

        return p;
    }

    public static Pipeline createFetchOnly(BuildTriplePatternCountTablePipelineOptions options) {
        Pipeline p = preparePipeline(options);

        p
                .apply(Create.of(options.getSource()))
                .apply(new AccumuloIO.Read(
                        options.getAccumuloName(), options.getZookeeperServers(),
                        options.getAccumuloUsername(), options.getAccumuloPassword()
                ))
                .apply(new TripleToProspects())
                .apply(MapElements.via(new SimpleFunction<Map.Entry<IntermediateProspect, Long>, AvroContainer>() {

                    @Override
                    public AvroContainer apply(Map.Entry<IntermediateProspect, Long> input) {
                        try {
                            return new AvroContainer().pack(input,
                                    MapEntryCoder.of(IntermediateProspectCoder.of(), VarLongCoder.of()));
                        } catch (CoderException ex) {
                            LOG.error(ex.getMessage(), ex);
                        }

                        return null;
                    }
                }))
                .apply(AvroIO.write(AvroContainer.class)
                        .to(options.getDestination())
                        .withSuffix(".avro")
                );

        return p;
    }

    public static Pipeline createCombinerAndWriter(BuildTriplePatternCountTablePipelineOptions options) {
        Pipeline p = preparePipeline(options);

        p
                .apply(Create.of(options.getSource()))
                .apply("Prepare tables",new PrepareDestinationTable<>())
                .apply(AvroIO.readAll(AvroContainer.class))
                .apply("Unpack prospect", ParDo.of(new DoFn<AvroContainer, Map.Entry<IntermediateProspect, Long>>() {

                    @ProcessElement
                    public void processElement(@Element AvroContainer input,
                                               OutputReceiver<Map.Entry<IntermediateProspect, Long>> receiver) {
                        try {
                            Map.Entry<IntermediateProspect, Long> prospect = (Map.Entry<IntermediateProspect, Long>)
                                    input.unpack(MapEntryCoder.of(IntermediateProspectCoder.of(), VarLongCoder.of()));

                            checkArgument(prospect != null, "Prospect can't be null!");
                            checkArgument(prospect.getKey() != null, "Prospect key can't be null!");
                            checkArgument(prospect.getValue() != null, "Prospect value can't be null!");

                            receiver.output(prospect);
                        } catch (CoderException ex) {
                            LOG.error(ex.getMessage(), ex);
                        }
                    }
                }))
                .setCoder(MapEntryCoder.of(IntermediateProspectCoder.of(), VarLongCoder.of()))
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply("Aggregate prospects", new AggregateProspects())
                .apply("Prospect to mutation", new ProspectToMutation())
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply("Write mutations", new AccumuloSingleTableWrite(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getDestination()
                ));

        return p;
    }

    private static Pipeline preparePipeline(BuildTriplePatternCountTablePipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        p.getCoderRegistry().registerCoderForClass(String.class, StringUtf8Coder.of());
        p.getCoderRegistry().registerCoderForClass(Range.class, RangeCoder.of());
        p.getCoderRegistry().registerCoderForClass(IntermediateProspect.class, IntermediateProspectCoder.of());
        p.getCoderRegistry().registerCoderForClass(Mutation.class, new MutationCoder());
        p.getCoderRegistry().registerCoderForClass(AvroContainer.class, AvroCoder.of(AvroContainer.class));

        return p;
    }

    private static class PrepareDestinationTable<T> extends PTransform<PCollection<T>, PCollection<T>> {
        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new DoFn<T, T>() {

                @ProcessElement
                public void processElement(ProcessContext ctx) throws Exception {
                    BuildTriplePatternCountTablePipelineOptions options = ctx.getPipelineOptions()
                            .as(BuildTriplePatternCountTablePipelineOptions.class);

                    final Instance instance = new ZooKeeperInstance(
                            options.getAccumuloName(), options.getZookeeperServers());
                    final Connector conn = instance.getConnector(
                            options.getAccumuloUsername(), new PasswordToken(options.getAccumuloPassword()));

                    // Create the output table if doesn't exist
                    boolean exists = conn.tableOperations().exists(options.getDestination());
                    if (!exists) {
                        LOG.info("Table {} doesn't exist. Will create it.", options.getDestination());
                        conn.tableOperations().create(options.getDestination(), new NewTableConfiguration()
                                .withoutDefaultIterators());
                    }
                    // Attach the iterator if it doesn't exist
                    exists = conn.tableOperations().listIterators(options.getDestination())
                            .containsKey("prospectsSumming");
                    if (!exists) {
                        LOG.info("Prospects summing iterator doesn't exist. Will attach it.");
                        IteratorSetting iteratorSetting = new IteratorSetting(
                                15,
                                "prospectsSumming",
                                SummingCombiner.class
                        );
                        LongCombiner.setEncodingType(iteratorSetting, LongCombiner.Type.STRING);
                        Combiner.setColumns(iteratorSetting,
                                Collections.singletonList(new IteratorSetting.Column(ProspectorConstants.COUNT)));
                        conn.tableOperations().attachIterator(options.getDestination(), iteratorSetting);
                    }

                    ctx.output(ctx.element());
                }
            }));
        }
    }

    private static class TripleToProspects extends PTransform<PCollection<Map.Entry<Key, Value>>, PCollection<Map.Entry<IntermediateProspect, Long>>> {
        @Override
        public PCollection<Map.Entry<IntermediateProspect, Long>> expand(PCollection<Map.Entry<Key, Value>> input) {
            return input
                    .apply(ParDo.of(new DoFn<Map.Entry<Key, Value>, Map.Entry<IntermediateProspect, Long>>() {
                        @ProcessElement
                        public void processElement(ProcessContext ctx) throws TripleRowResolverException {
                            final Map.Entry<Key, Value> kv = ctx.element();
                            try {
                                RyaTripleContext rtc = new RyaTripleContext(false);
                                RyaStatement statement = rtc
                                        .deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                                                new TripleRow(
                                                        kv.getKey().getRow().getBytes(),
                                                        kv.getKey().getColumnFamily().getBytes(),
                                                        kv.getKey().getColumnQualifier().getBytes(),
                                                        kv.getKey().getTimestamp(),
                                                        kv.getKey().getColumnVisibility().getBytes(),
                                                        kv.getValue().get()
                                                ));

                                final String subject = statement.getSubject().getData();
                                final String predicate = statement.getPredicate().getData();
                                final String subjpred = statement.getSubject().getData() + IndexWorkPlan.DELIM
                                        + statement.getPredicate().getData();
                                final String predobj = statement.getPredicate().getData() + IndexWorkPlan.DELIM
                                        + statement.getObject().getData();
                                final String subjobj = statement.getSubject().getData() + IndexWorkPlan.DELIM
                                        + statement.getObject().getData();
                                final RyaType object = statement.getObject();
                                final int localIndex = URIUtil.getLocalNameIndex(subject);
                                final String namespace = subject.substring(0, localIndex - 1);
                                final String visibility = new String(statement.getColumnVisibility(), StandardCharsets.UTF_8);

                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(subject)
                                                        .setDataType(IndexWorkPlan.URITYPE)
                                                        .setTripleValueType(TripleValueType.SUBJECT)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(predicate)
                                                        .setDataType(IndexWorkPlan.URITYPE)
                                                        .setTripleValueType(TripleValueType.PREDICATE)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(object.getData())
                                                        .setDataType(object.getDataType().stringValue())
                                                        .setTripleValueType(TripleValueType.OBJECT)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(subjpred)
                                                        .setDataType(XMLSchema.STRING.toString())
                                                        .setTripleValueType(TripleValueType.SUBJECT_PREDICATE)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(subjobj)
                                                        .setDataType(XMLSchema.STRING.toString())
                                                        .setTripleValueType(TripleValueType.SUBJECT_OBJECT)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(predobj)
                                                        .setDataType(XMLSchema.STRING.toString())
                                                        .setTripleValueType(TripleValueType.PREDICATE_OBJECT)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                                ctx.output(
                                        new HashMap.SimpleEntry<>(
                                                IntermediateProspect.builder()
                                                        .setIndex(ProspectorConstants.COUNT)
                                                        .setData(namespace)
                                                        .setDataType(IndexWorkPlan.URITYPE)
                                                        .setTripleValueType(TripleValueType.ENTITY)
                                                        .setVisibility(visibility)
                                                        .build(),
                                                1L
                                        )
                                );
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);

                                throw e;
                            }
                        }
                    }))
                    .setCoder(MapEntryCoder.of(WritableCoder.of(IntermediateProspect.class), VarLongCoder.of()));
        }
    }

    private static class ProspectToMutation extends PTransform<PCollection<Map.Entry<IntermediateProspect, Long>>, PCollection<Mutation>> {
        @Override
        public PCollection<Mutation> expand(PCollection<Map.Entry<IntermediateProspect, Long>> input) {
            return input.apply(MapElements.via(new SimpleFunction<Map.Entry<IntermediateProspect, Long>, Mutation>() {

                @Override
                public Mutation apply(Map.Entry<IntermediateProspect, Long> input) {
                    final IntermediateProspect prospect = input.getKey();
                    final Long prospectCount = input.getValue();

                    checkArgument(prospect != null, "Prospect can't be null!");
                    checkArgument(prospectCount != null, "Prospect count can't be null!");

                    final String indexType = prospect.getTripleValueType().getIndexType();

                    final Mutation mutation = new Mutation(
                            indexType + IndexWorkPlan.DELIM + prospect.getData());

                    final ColumnVisibility visibility = new ColumnVisibility(prospect.getVisibility());
                    final Value sumValue = new Value(new LongCombiner.StringEncoder().encode(prospectCount));

                    mutation.put(COUNT, prospect.getDataType(), visibility, System.currentTimeMillis(), sumValue);

                    return mutation;
                }
            }));
        }
    }

    private static class AggregateProspects extends PTransform<PCollection<Iterable<Map.Entry<IntermediateProspect, Long>>>, PCollection<Map.Entry<IntermediateProspect, Long>>> {
        @Override
        public PCollection<Map.Entry<IntermediateProspect, Long>> expand(PCollection<Iterable<Map.Entry<IntermediateProspect, Long>>> input) {
            return input.apply(ParDo.of(new DoFn<Iterable<Map.Entry<IntermediateProspect, Long>>, Map.Entry<IntermediateProspect, Long>>() {

                @ProcessElement
                public void processElement(ProcessContext ctx) {
                    Map<IntermediateProspect, List<Map.Entry<IntermediateProspect, Long>>> grouped = StreamSupport
                            .stream(ctx.element().spliterator(), true)
                            .collect(Collectors.groupingBy(Map.Entry::getKey));

                    List<Map.Entry<IntermediateProspect, Long>> aggregated = grouped.entrySet()
                            .parallelStream()
                            .map(it -> it.getValue().stream()
                                    .reduce((a, b) -> new HashMap.SimpleEntry<>(a.getKey(), a.getValue() + b.getValue()))
                                    .orElseThrow(() -> new IllegalStateException("Count can't be zero!")))
                            .collect(Collectors.toList());

                    for (Map.Entry<IntermediateProspect, Long> entry : aggregated) {
                        ctx.output(entry);
                    }
                }
            }));
        }
    }

    /**
     * There are two options to run the pipeline:
     * * Option A - read from the SPO index and write to the prospects index without an intermediate step,
     * * Option B - do the same, but with an intermediate step that stores the index in a Google Storage.
     */
    public static void main(String[] args) throws ClassNotFoundException {
        BuildTriplePatternCountTablePipelineOptions options = PipelineOptionsFactory
                .as(BuildTriplePatternCountTablePipelineOptions.class);

        options.setProject("core-datafabric");
        options.setRegion("europe-west1");
        options.setTempLocation("gs://datafabric-dataflow/temp");
        options.setGcpTempLocation("gs://datafabric-dataflow/staging");

        options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.dataflow.DataflowRunner"));
//      options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.direct.DirectRunner"));

        options.setAccumuloName("accumulo");
        options.setZookeeperServers("10.132.0.18:2181");
        options.setAccumuloUsername("root");
        options.setAccumuloPassword("accumulo");

        /*
         * Option A:
         * To read from the SPO index and write to the prospects index
         */
//        options.setJobName("rya-prospects");
//        options.setMaxNumWorkers(40);
//
//        options.setBatchSize(1000000);
//
//        options.setSource("triplestore_spo");
//        options.setDestination("triplestore_prospects");
//        Pipeline p = BuildTriplePatternCountTablePipeline.create(options);
//        p.run();

        /*
         * Option B:
         */

        /*
         * To read from the SPO index and write the prospects to files.
         */
//        options.setJobName("rya-prospects-fetchonly");
//        options.setMaxNumWorkers(40);
//
//        options.setSource("triplestore_spo");
//        options.setDestination("gs://datafabric-rya-dev/prospects/prospect");
//
//        options.setBatchSize(500000);
//
//        Pipeline p = BuildTriplePatternCountTablePipeline.createFetchOnly(options);
//        p.run();

        /*
         * To read prospects from files, aggregate and write them to a table.
         */
        options.setJobName("rya-prospects-combineandwrite");
        options.setMaxNumWorkers(20);

        options.setSource("gs://datafabric-rya-dev/prospects/prospect-*.avro");
        options.setDestination("triplestore_prospects");

        options.setBatchSize(500000);

        Pipeline p = BuildTriplePatternCountTablePipeline.createCombinerAndWriter(options);
        p.run();
    }

}
