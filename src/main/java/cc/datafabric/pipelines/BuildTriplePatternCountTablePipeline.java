package cc.datafabric.pipelines;

import cc.datafabric.pipelines.options.DefaultRyaPipelineOptions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import org.apache.rya.prospector.utils.ProspectorUtils;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.apache.rya.prospector.utils.ProspectorConstants.*;

interface BuildTriplePatternCountTablePipelineOptions extends DefaultRyaPipelineOptions {

    String getInTable();

    void setInTable(String tableId);

    String getOutTable();

    void setOutTable(String tableId);

    long getBatchSize();

    void setBatchSize(long batchSize);

    int getNumParallelBatches();

    void setNumParallelBatches(int numParallelBatches);

    String getVersionDate();

    void setVersionDate(String dateTime);

}

class BuildTriplePatternCountTablePipeline {

    private static final Logger LOG = LoggerFactory.getLogger(BuildTriplePatternCountTablePipeline.class);

    public static Pipeline create(BuildTriplePatternCountTablePipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        int numParallelBatches = options.getNumParallelBatches();
        String versionDateAsString = options.getVersionDate();

        p.getCoderRegistry().registerCoderForClass(String.class, StringUtf8Coder.of());
        p.getCoderRegistry().registerCoderForClass(Range.class, WritableCoder.of(Range.class));
        p.getCoderRegistry().registerCoderForClass(IntermediateProspect.class, WritableCoder.of(IntermediateProspect.class));
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

                        List<Text> splits = Arrays.asList(conn.tableOperations()
                                .listSplits(options.getInTable())
                                .toArray(new Text[0]));

                        if (splits.isEmpty()) {
                            // Then we're going to read the whole table at once
                            LOG.info("Table {} doesn't have splits!", options.getInTable());

                            ctx.output(new Range());
                        } else {
                            LOG.info("Table {}. Found {} splits", options.getInTable(), splits.size());
                            int index = 0;
                            while (index < splits.size()) {
                                Range range;
                                if (index == 0) {
                                    range = new Range(
                                            null, true,
                                            splits.get(index + 1), false);
                                } else if (index < splits.size() - 1) {
                                    range = new Range(
                                            splits.get(index), true,
                                            splits.get(index + 1), false);
                                } else {
                                    range = new Range(
                                            splits.get(index), true,
                                            null, false);
                                }

                                ctx.output(range);

                                index++;
                            }
                        }
                    }
                }))
                .apply("Read rows", ParDo.of(new DoFn<Range, KV<Key, Value>>() {

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

                            scanner.forEach(it -> ctx.output(KV.of(it.getKey(), it.getValue())));
                        }
                    }

                }))
                .apply("Create intermediate prospects", ParDo.of(new DoFn<KV<Key, Value>, KV<String, KV<IntermediateProspect, Long>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws TripleRowResolverException {
                        final KV<Key, Value> kv = ctx.element();
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

                            ctx.output(KV.of(subject,
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(subject)
                                                    .setDataType(IndexWorkPlan.URITYPE)
                                                    .setTripleValueType(TripleValueType.SUBJECT)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                            ctx.output(KV.of(predicate,
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(predicate)
                                                    .setDataType(IndexWorkPlan.URITYPE)
                                                    .setTripleValueType(TripleValueType.PREDICATE)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                            ctx.output(KV.of(object.getData(),
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(object.getData())
                                                    .setDataType(object.getDataType().stringValue())
                                                    .setTripleValueType(TripleValueType.OBJECT)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                            ctx.output(KV.of(subjpred,
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(subjpred)
                                                    .setDataType(XMLSchema.STRING.toString())
                                                    .setTripleValueType(TripleValueType.SUBJECT_PREDICATE)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                            ctx.output(KV.of(subjobj,
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(subjobj)
                                                    .setDataType(XMLSchema.STRING.toString())
                                                    .setTripleValueType(TripleValueType.SUBJECT_OBJECT)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                            ctx.output(KV.of(predobj,
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(predobj)
                                                    .setDataType(XMLSchema.STRING.toString())
                                                    .setTripleValueType(TripleValueType.PREDICATE_OBJECT)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                            ctx.output(KV.of(namespace,
                                    KV.of(
                                            IntermediateProspect.builder()
                                                    .setIndex(ProspectorConstants.COUNT)
                                                    .setData(namespace)
                                                    .setDataType(IndexWorkPlan.URITYPE)
                                                    .setTripleValueType(TripleValueType.ENTITY)
                                                    .setVisibility(visibility)
                                                    .build(),
                                            1L
                                    ))
                            );
                        } catch (Throwable e) {
                            LOG.error(e.getMessage(), e);

                            throw e;
                        }
                    }
                }))
                .apply("Aggregate prospects", Combine.perKey((Iterable<KV<IntermediateProspect, Long>> it) ->
                        StreamSupport.stream(it.spliterator(), true)
                                .reduce((a, b) -> KV.of(a.getKey(), a.getValue() + b.getValue()))
                                .get())
                )
                .apply(Values.create())
                .apply("Convert prospect to mutation", MapElements.via(new SimpleFunction<KV<IntermediateProspect, Long>, KV<String, Mutation>>() {

                    @Override
                    public KV<String, Mutation> apply(KV<IntermediateProspect, Long> input) {

                        final IntermediateProspect prospect = input.getKey();
                        final Long prospectCount = input.getValue();
                        final String indexType = prospect.getTripleValueType().getIndexType();
                        final Date versionDate = Date.from(Instant.parse(versionDateAsString));

                        final Mutation mutation = new Mutation(
                                indexType + IndexWorkPlan.DELIM + prospect.getData()
                                        + IndexWorkPlan.DELIM + ProspectorUtils.getReverseIndexDateTime(versionDate));

                        final ColumnVisibility visibility = new ColumnVisibility(prospect.getVisibility());
                        final Value sumValue = new Value(prospectCount.toString().getBytes(StandardCharsets.UTF_8));

                        mutation.put(COUNT, prospect.getDataType(), visibility, versionDate.getTime(), sumValue);

                        int shardNum = numParallelBatches > 0 ? numParallelBatches : 1;

                        return KV.of(
                                "shard-" + (System.currentTimeMillis() / 1000) % shardNum,
                                mutation
                        );
                    }
                }))
                .apply(GroupIntoBatches.ofSize(options.getBatchSize()))
                .apply("Write mutations", ParDo.of(new DoFn<KV<String, Iterable<Mutation>>, KV<String, Boolean>>() {

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

                        ctx.output(KV.of("", true));
                    }

                }))
                // Force to wait the end of the upstream
                .apply(GroupByKey.create())
                .apply("Write finalizing mutation", ParDo.of(new DoFn<KV<String, Iterable<Boolean>>, Boolean>() {

                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws Exception {
                        final BuildTriplePatternCountTablePipelineOptions options = ctx.getPipelineOptions()
                                .as(BuildTriplePatternCountTablePipelineOptions.class);
                        final Date versionDate = Date.from(Instant.parse(options.getVersionDate()));

                        final Mutation mutation = new Mutation(METADATA);
                        mutation.put(PROSPECT_TIME,
                                ProspectorUtils.getReverseIndexDateTime(versionDate),
                                new ColumnVisibility(ProspectorConstants.DEFAULT_VIS),
                                versionDate.getTime(),
                                new Value(ProspectorConstants.EMPTY)
                        );
                        final Instance instance = new ZooKeeperInstance(
                                options.getAccumuloName(), options.getZookeeperServers());
                        final Connector conn = instance.getConnector(
                                options.getAccumuloUsername(), new PasswordToken(options.getAccumuloPassword()));
                        final BatchWriterConfig config = new BatchWriterConfig();

                        try (BatchWriter writer = conn.createBatchWriter(options.getOutTable(), config)) {
                            writer.addMutation(mutation);
                        }
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
        options.setMaxNumWorkers(10);

        options.setAccumuloName("accumulo");
        options.setZookeeperServers("10.132.0.18:2181");
        options.setAccumuloUsername("root");
        options.setAccumuloPassword("accumulo");
        options.setInTable("triplestore_spo");
        options.setOutTable("triplestore_prospects");

        options.setVersionDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
        options.setBatchSize(50000);
        options.setNumParallelBatches(5); // num tservers * 3 - 1

        Pipeline p = BuildTriplePatternCountTablePipeline.create(options);

        p.run();
    }

}
