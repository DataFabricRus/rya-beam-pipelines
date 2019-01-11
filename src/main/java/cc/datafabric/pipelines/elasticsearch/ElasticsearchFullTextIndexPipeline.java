package cc.datafabric.pipelines.elasticsearch;

import cc.datafabric.pipelines.io.*;
import cc.datafabric.pipelines.options.DefaultRyaPipelineOptions;
import cc.datafabric.pipelines.transforms.GroupIntoLocalBatches;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.rya.accumulo.iterators.RDFPropertyFilter;
import org.apache.rya.accumulo.query.KeyValueToRyaStatementFunction;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.impl.RyaIRIResolver;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.elasticsearch.ElasticsearchDocument;
import org.eclipse.rdf4j.sail.elasticsearch.ElasticsearchIndex;
import org.eclipse.rdf4j.sail.lucene.BulkUpdater;
import org.eclipse.rdf4j.sail.lucene.SearchFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

interface ElasticsearchFullTextIndexPipelineOptions extends DefaultRyaPipelineOptions {

    /**
     * The SPO table name.
     */
    String getSource();

    void setSource(String source);

    String getElasticsearchHost();

    void setElasticsearchHost(String elasticsearchHost);

    String[] getProperties();

    void setProperties(String[] properties);

    int getBatchSize();

    void setBatchSize(int batchSize);

    String getStartDateTime();

    /**
     * If not set, will try to read it from Zookeeper.
     */
    void setStartDateTime(String startDateTime);

    String getEndDateTime();

    void setEndDateTime(String endDateTime);
}

class ElasticsearchFullTextIndexPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchFullTextIndexPipeline.class);

    public static Pipeline create(ElasticsearchFullTextIndexPipelineOptions options) {
        Pipeline p = preparePipeline(options);

        p
                .apply(Create.of(options.getSource()))
                .apply("Fetch Splits", new AccumuloFetchSplits(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword()
                ))
                .apply(Reshuffle.viaRandomKey())
                .apply(new AccumuloReadAndGroupByRyaSubject(
                        options.getAccumuloName(), options.getZookeeperServers(),
                        options.getAccumuloUsername(), options.getAccumuloPassword(),
                        1000, RDFPropertyFilter.class,
                        MapHelpers.singletonMap(RDFPropertyFilter.OPTION_PROPERTIES, options.getProperties())
                ))
                .apply(new SubjectGroupToStatementGroup())
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply(new WriteStatementToIndex(options.getElasticsearchHost()))
                .apply(new WaitEndAndWriteTimestamp(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getEndDateTime()
                ));

        return p;
    }

    public static Pipeline createWithTimestampFilter(ElasticsearchFullTextIndexPipelineOptions options) {
        Pipeline p = preparePipeline(options);

        p
                .apply(Create.of(options.getSource()))
                .apply("Fetch Splits", new AccumuloFetchSplits(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword()
                ))
                .apply(Reshuffle.viaRandomKey())
                .apply("Filter by start and end time", new AccumuloFilterRangesByTimestamp(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getStartDateTime(),
                        options.getEndDateTime(),
                        options.getProperties()
                ))
                .apply(new AccumuloReadAndGroupByRyaSubject(
                        options.getAccumuloName(), options.getZookeeperServers(),
                        options.getAccumuloUsername(), options.getAccumuloPassword(),
                        1000, RDFPropertyFilter.class,
                        MapHelpers.singletonMap(RDFPropertyFilter.OPTION_PROPERTIES, options.getProperties())
                ))
                .apply(new SubjectGroupToStatementGroup())
                .apply(GroupIntoLocalBatches.of(options.getBatchSize()))
                .apply(new WriteStatementToIndex(options.getElasticsearchHost()))
                .apply(new WaitEndAndWriteTimestamp(
                        options.getAccumuloName(),
                        options.getZookeeperServers(),
                        options.getAccumuloUsername(),
                        options.getAccumuloPassword(),
                        options.getEndDateTime()
                ));

        return p;
    }

    private static class SubjectGroupToStatementGroup
            extends PTransform<PCollection<KV<String, Iterable<Map.Entry<Key, Value>>>>, PCollection<KV<String, Iterable<Statement>>>> {

        @Override
        public PCollection<KV<String, Iterable<Statement>>> expand(PCollection<KV<String, Iterable<Map.Entry<Key, Value>>>> input) {
            return input.apply(ParDo.of(new DoFn<KV<String, Iterable<Map.Entry<Key, Value>>>, KV<String, Iterable<Statement>>>() {

                private transient ValueFactory vf;
                private transient KeyValueToRyaStatementFunction function;

                @Setup
                public void setup() throws Exception {
                    vf = RdfCloudTripleStoreConstants.VALUE_FACTORY;
                    function = new KeyValueToRyaStatementFunction(
                            RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, new RyaTripleContext(false));
                }

                @ProcessElement
                public void processElement(@Element KV<String, Iterable<Map.Entry<Key, Value>>> input,
                                           OutputReceiver<KV<String, Iterable<Statement>>> receiver) {

                    List<Statement> statements = StreamSupport.stream(input.getValue().spliterator(), true)
                            .map(it -> {
                                RyaStatement rs = function.apply(it);
                                if (rs != null) {
                                    final org.eclipse.rdf4j.model.Value objectValue;
                                    if (rs.getObject().getDataType() == new RyaIRIResolver().getRyaDataType()) {
                                        objectValue = vf.createIRI(rs.getObject().getData());
                                    } else {
                                        if (rs.getObject().getLanguage() == null) {
                                            objectValue = vf.createLiteral(rs.getObject().getData(),
                                                    rs.getObject().getDataType());
                                        } else {
                                            objectValue = vf.createLiteral(rs.getObject().getData(),
                                                    rs.getObject().getLanguage());
                                        }
                                    }

                                    if (rs.getContext() == null) {
                                        return vf.createStatement(
                                                vf.createIRI(rs.getSubject().getData()),
                                                vf.createIRI(rs.getPredicate().getData()),
                                                objectValue
                                        );
                                    } else {
                                        return vf.createStatement(
                                                vf.createIRI(rs.getSubject().getData()),
                                                vf.createIRI(rs.getPredicate().getData()),
                                                objectValue,
                                                vf.createIRI(rs.getContext().getData())
                                        );
                                    }
                                }

                                return null;
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    receiver.output(KV.of(input.getKey(), statements));
                }
            }));
        }
    }

    private static class WriteStatementToIndex
            extends PTransform<PCollection<Iterable<KV<String, Iterable<Statement>>>>, PCollection<Boolean>> {

        private final String elasticsearchHost;

        WriteStatementToIndex(String elasticsearchHost) {
            this.elasticsearchHost = elasticsearchHost;
        }

        @Override
        public PCollection<Boolean> expand(PCollection<Iterable<KV<String, Iterable<Statement>>>> input) {
            return input.apply(ParDo.of(new Write()));
        }

        private class Write extends DoFn<Iterable<KV<String, Iterable<Statement>>>, Boolean> {

            private OpenElasticsearchIndex index;

            @Setup
            public void setup() throws Exception {
                Properties properties = new Properties();
                properties.setProperty(ElasticsearchIndex.TRANSPORT_KEY, elasticsearchHost);

                index = new OpenElasticsearchIndex();
                index.initialize(properties);

                LOG.info("Connected to Elasticsearch on {}", elasticsearchHost);
            }

            @Teardown
            public void tearDown() throws IOException {
                index.shutDown();
            }

            @ProcessElement
            public void processElement(
                    @Element Iterable<KV<String, Iterable<Statement>>> documents, OutputReceiver<Boolean> receiver
            ) throws IOException {
                long start = System.currentTimeMillis();

                BulkUpdater bulkUpdater = index.newBulkUpdate();

                for (KV<String, Iterable<Statement>> entry : documents) {
                    final ElasticsearchDocument doc = new ElasticsearchDocument(
                            SearchFields.formIdString(Objects.requireNonNull(entry.getKey()), null),
                            ElasticsearchIndex.DEFAULT_DOCUMENT_TYPE,
                            ElasticsearchIndex.DEFAULT_INDEX_NAME,
                            entry.getKey(),
                            SearchFields.getContextID(null),
                            null
                    );

                    entry.getValue().forEach(it -> {
                        doc.addProperty(
                                SearchFields.getPropertyField(it.getPredicate()),
                                SearchFields.getLiteralPropertyValueAsString(it)
                        );
                    });

                    bulkUpdater.add(doc);
                }

                bulkUpdater.end();

                receiver.output(true);

                LOG.info("Wrote a batch in {} ms", System.currentTimeMillis() - start);
            }

        }
    }

    private static Pipeline preparePipeline(ElasticsearchFullTextIndexPipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        p.getCoderRegistry().registerCoderForClass(IteratorSetting.class, WritableCoder.of(IteratorSetting.class));

        return p;
    }

    private static class OpenElasticsearchIndex extends ElasticsearchIndex {

        public OpenElasticsearchIndex() {
            super();
        }

        @Override
        public BulkUpdater newBulkUpdate() {
            return super.newBulkUpdate();
        }

    }

    public static void main(String[] args) throws ClassNotFoundException {
        ElasticsearchFullTextIndexPipelineOptions options = PipelineOptionsFactory
                .as(ElasticsearchFullTextIndexPipelineOptions.class);

        options.setJobName("rya-fulltext");
        options.setProject("core-datafabric");
        options.setRegion("europe-west1");
        options.setTempLocation("gs://datafabric-dataflow/temp");
        options.setGcpTempLocation("gs://datafabric-dataflow/staging");
        options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.dataflow.DataflowRunner"));
//        options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.direct.DirectRunner"));
        options.setMaxNumWorkers(5);

        options.setAccumuloName("accumulo");
        options.setZookeeperServers("rya-cluster-a-m:2181");
        options.setAccumuloUsername("root");
        options.setAccumuloPassword("accumulo");

        options.setSource("triplestore_spo");
        options.setElasticsearchHost("rya-cluster-e");
        options.setBatchSize(100);
        options.setProperties(new String[]{
                //"http://www.w3.org/2000/01/rdf-schema#label",
                "http://www.w3.org/2004/02/skos/core#prefLabel",
                "http://www.w3.org/2004/02/skos/core#notation",
                //"https://spec.edmcouncil.org/fibo/ontology/FND/AgentsAndPeople/Agents/hasName",
                //"https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/hasLegalName",
                //"https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/hasAlias",
                //"https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/hasUniqueIdentifier"
        });

//        Pipeline p = ElasticsearchFullTextIndexPipeline.create(options);

        options.setStartDateTime("2018-01-01T00:00:00Z");
        options.setEndDateTime(DateTimeFormatter.ISO_DATE_TIME.format(ZonedDateTime.now()));
        Pipeline p = ElasticsearchFullTextIndexPipeline.createWithTimestampFilter(options);

        p.run();
    }

}
