package cc.datafabric.pipelines;

import cc.datafabric.pipelines.coders.IntermediateProspectCoder;
import cc.datafabric.pipelines.coders.MapEntryCoder;
import cc.datafabric.pipelines.coders.RangeCoder;
import cc.datafabric.pipelines.options.DefaultRyaPipelineOptions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;
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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.rya.prospector.utils.ProspectorConstants.COUNT;

interface BuildTriplePatternCountTablePipelineOptions extends DefaultRyaPipelineOptions {

    String getInTable();

    void setInTable(String tableId);

    String getOutTable();

    void setOutTable(String tableId);

    long getBatchSize();

    void setBatchSize(long batchSize);

    int getNumParallelBatches();

    void setNumParallelBatches(int numParallelBatches);

}

class BuildTriplePatternCountTablePipeline {

    private static final Logger LOG = LoggerFactory.getLogger(BuildTriplePatternCountTablePipeline.class);

    public static Pipeline create(BuildTriplePatternCountTablePipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        int numParallelBatches = options.getNumParallelBatches();

        p.getCoderRegistry().registerCoderForClass(String.class, StringUtf8Coder.of());
        p.getCoderRegistry().registerCoderForClass(Range.class, RangeCoder.of());
        p.getCoderRegistry().registerCoderForClass(IntermediateProspect.class, new IntermediateProspectCoder());
        p.getCoderRegistry().registerCoderForClass(Mutation.class, WritableCoder.of(Mutation.class));

        p
                .apply(Create.of(""))
                .apply("Fetch splits", ParDo.of(new DoFn<String, Range>() {

                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws Exception {
                        BuildTriplePatternCountTablePipelineOptions options = ctx.getPipelineOptions()
                                .as(BuildTriplePatternCountTablePipelineOptions.class);

                        final Instance instance = new ZooKeeperInstance(
                                options.getAccumuloName(), options.getZookeeperServers());
                        final Connector conn = instance.getConnector(
                                options.getAccumuloUsername(), new PasswordToken(options.getAccumuloPassword()));

                        // Create the output table if doesn't exist
                        boolean exists = conn.tableOperations().exists(options.getOutTable());
                        if (!exists) {
                            LOG.info("Table {} doesn't exist. Will create it.", options.getOutTable());
                            conn.tableOperations().create(options.getOutTable(), new NewTableConfiguration()
                                    .withoutDefaultIterators());
                        }
                        // Attach the iterator if it doesn't exist
                        exists = conn.tableOperations().listIterators(options.getOutTable())
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
                            conn.tableOperations().attachIterator(options.getOutTable(), iteratorSetting);
                        }

                        List<Text> splits = Arrays.asList(conn.tableOperations()
                                .listSplits(options.getInTable())
                                .toArray(new Text[0]));

                        if (splits.isEmpty()) {
                            // Then we're going to read the whole table at once
                            LOG.info("Table {}. There is no splits!", options.getOutTable());

                            ctx.output(new Range());
                        } else {
                            LOG.info("Table {}. Found {} splits", options.getOutTable(), splits.size());
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

                                ctx.output(range);

                                index++;
                            }

                            ctx.output(new Range(splits.get(index - 1), true,
                                    null, false));
                        }
                    }
                }))
                .apply(Reshuffle.viaRandomKey())
                .apply("Read rows", ParDo.of(new DoFn<Range, Map.Entry<Key, Value>>() {

                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws Exception {
                        BuildTriplePatternCountTablePipelineOptions options = ctx.getPipelineOptions()
                                .as(BuildTriplePatternCountTablePipelineOptions.class);
                        Range range = ctx.element();

                        Instance instance = new ZooKeeperInstance(options.getAccumuloName(),
                                options.getZookeeperServers());

                        Connector conn = instance.getConnector(
                                options.getAccumuloUsername(), new PasswordToken(options.getAccumuloPassword()));

                        try (Scanner scanner = conn.createScanner(options.getInTable(), Authorizations.EMPTY)) {
                            scanner.setRange(range);

                            scanner.forEach(ctx::output);
                        }
                    }

                }))
                .setCoder(new MapEntryCoder<>(WritableCoder.of(Key.class), WritableCoder.of(Value.class)))
                .apply("Create intermediate prospects", ParDo.of(new DoFn<Map.Entry<Key, Value>, Map.Entry<IntermediateProspect, Long>>() {
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
                .setCoder(new MapEntryCoder<>(WritableCoder.of(IntermediateProspect.class), VarLongCoder.of()))
                .apply("Partition prospects", ParDo.of(new DoFn<Map.Entry<IntermediateProspect, Long>, KV<String, Map.Entry<IntermediateProspect, Long>>>() {

                    @ProcessElement
                    public void apply(@Element Map.Entry<IntermediateProspect, Long> input, OutputReceiver<KV<String, Map.Entry<IntermediateProspect, Long>>> receiver) {
                        final int numShards = numParallelBatches > 0 ? numParallelBatches : 1;
                        final String shardId = "shard-" + ((System.currentTimeMillis() / 100) % numShards);

                        receiver.output(KV.of(shardId, input));
                    }
                }))
                .apply(GroupIntoBatches.ofSize(options.getBatchSize()))
                .apply("Aggregate prospects", ParDo.of(new DoFn<KV<String, Iterable<Map.Entry<IntermediateProspect, Long>>>, Map.Entry<IntermediateProspect, Long>>() {

                    @ProcessElement
                    public void processElement(ProcessContext ctx) {
                        StreamSupport.stream(ctx.element().getValue().spliterator(), true)
                                .collect(Collectors.groupingBy(Map.Entry::getKey))
                                .entrySet()
                                .parallelStream()
                                .map(it -> it.getValue().stream()
                                        .reduce((a, b) -> new HashMap.SimpleEntry<>(a.getKey(), a.getValue() + b.getValue()))
                                        .orElseThrow(() -> new IllegalStateException("Count can't be zero!")))
                                .forEach(ctx::output);
                    }
                }))
                .apply("Convert prospect to mutation", MapElements.via(new SimpleFunction<Map.Entry<IntermediateProspect, Long>, KV<String, Mutation>>() {

                    @Override
                    public KV<String, Mutation> apply(Map.Entry<IntermediateProspect, Long> input) {
                        final IntermediateProspect prospect = input.getKey();
                        final Long prospectCount = input.getValue();
                        final String indexType = prospect.getTripleValueType().getIndexType();

                        final Mutation mutation = new Mutation(
                                indexType + IndexWorkPlan.DELIM + prospect.getData());

                        final ColumnVisibility visibility = new ColumnVisibility(prospect.getVisibility());
                        final Value sumValue = new Value(new LongCombiner.StringEncoder().encode(prospectCount));

                        mutation.put(COUNT, prospect.getDataType(), visibility, System.currentTimeMillis(), sumValue);

                        int shardNum = numParallelBatches > 0 ? numParallelBatches : 1;

                        return KV.of("shard-" + ((System.currentTimeMillis() / 100) % shardNum), mutation);
                    }
                }))
                .apply(GroupIntoBatches.ofSize(options.getBatchSize()))
                .apply("Write mutations", ParDo.of(new DoFn<KV<String, Iterable<Mutation>>, Boolean>() {

                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws Exception {
                        final Iterable<Mutation> mutations = ctx.element().getValue();
                        final BuildTriplePatternCountTablePipelineOptions options = ctx.getPipelineOptions()
                                .as(BuildTriplePatternCountTablePipelineOptions.class);

                        final Instance instance = new ZooKeeperInstance(
                                options.getAccumuloName(), options.getZookeeperServers());
                        final Connector conn = instance.getConnector(
                                options.getAccumuloUsername(), new PasswordToken(options.getAccumuloPassword()));
                        final BatchWriterConfig config = new BatchWriterConfig();

                        try (BatchWriter writer = conn.createBatchWriter(options.getOutTable(), config)) {
                            writer.addMutations(mutations);
                        }

                        ctx.output(true);
                    }

                }));

        return p;
    }

    public static void main(String[] args) throws ClassNotFoundException {
        BuildTriplePatternCountTablePipelineOptions options = PipelineOptionsFactory
                .as(BuildTriplePatternCountTablePipelineOptions.class);

        options.setJobName("rya-prospects");
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
        options.setInTable("triplestore_spo");
        options.setOutTable("triplestore_prospects");

        options.setBatchSize(1000000);
        options.setNumParallelBatches(10);

        Pipeline p = BuildTriplePatternCountTablePipeline.create(options);

        p.run();
    }

}
