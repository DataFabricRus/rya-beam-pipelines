package cc.datafabric.pipelines.statistics;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.prospector.domain.IndexEntry;
import org.apache.rya.prospector.domain.TripleValueType;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.service.ProspectorService;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.prospector.utils.ProspectorConstants;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class StatisticsPipelineTest {

    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "pass";
    private static final String IN_TABLE = "rya_spo";
    private static final String OUT_TABLE = "rya_prospects";
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private MiniAccumuloCluster accumulo;
    private File tempDir;
    private Connector connector;

    @Before
    public void before() throws Exception {
        tempDir = new File(FileUtils.getTempDirectory(), "accumulo-" + System.currentTimeMillis());
        assert tempDir.mkdirs();

        accumulo = new MiniAccumuloCluster(tempDir, ACCUMULO_PASSWORD);
        accumulo.getConfig().setZooKeeperStartupTime(60000);
        accumulo.start();

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        connector = instance.getConnector(ACCUMULO_USERNAME, new PasswordToken(ACCUMULO_PASSWORD));
        connector.securityOperations().changeUserAuthorizations(ACCUMULO_USERNAME, new Authorizations("U", "FOUO"));

        if (connector.tableOperations().exists(OUT_TABLE)) {
            connector.tableOperations().delete(OUT_TABLE);
        }
        NewTableConfiguration conf = new NewTableConfiguration()
                .withoutDefaultIterators();
        connector.tableOperations().create(OUT_TABLE, conf);

        IteratorSetting iteratorSetting = new IteratorSetting(
                15,
                "prospectsSumming",
                SummingCombiner.class
        );
        LongCombiner.setEncodingType(iteratorSetting, LongCombiner.Type.STRING);
        Combiner.setColumns(iteratorSetting, Collections.singletonList(new IteratorSetting.Column(ProspectorConstants.COUNT)));
        connector.tableOperations().attachIterator(OUT_TABLE, iteratorSetting);
    }

    @After
    public void after() throws IOException, InterruptedException {
        accumulo.stop();
        FileUtils.forceDelete(tempDir);
    }

    @Test
    public void testWithAvroAsIntermediate() throws Exception {
        // Load some data into Accumulo
        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata1"))); //2
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata2"))); //3
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("12"))); //1
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12"))); //4
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred1"), new RyaType("12"))); //5

        ryaDAO.destroy();

        SortedSet<Text> splits = new TreeSet<>();
        splits.add(new Text("urn:gem:etype#1234"));
        connector.tableOperations().addSplits(IN_TABLE, splits);

        // Run the pipeline
        StatisticsPipelineOptions options = PipelineOptionsFactory
                .as(StatisticsPipelineOptions.class);

        options.setAccumuloName(accumulo.getInstanceName());
        options.setZookeeperServers(accumulo.getZooKeepers());
        options.setAccumuloUsername(ACCUMULO_USERNAME);
        options.setAccumuloPassword(ACCUMULO_PASSWORD);
        options.setSource(IN_TABLE);
        options.setDestination(tempDir.getAbsolutePath() + "/pipeline-output/prospect");
        options.setBatchSize(2);

        StatisticsPipeline
                .createFetchOnly(options)
                .run()
                .waitUntilFinish();

        options.setSource(tempDir.getAbsolutePath() + "/pipeline-output/prospect-*.avro");
        options.setDestination(OUT_TABLE);

        StatisticsPipeline
                .createCombinerAndWriter(options)
                .run()
                .waitUntilFinish();

        final ProspectorService service = new ProspectorService(connector, OUT_TABLE);
        final String[] auths = {"U", "FOUO"};

        // Ensure one of the correct "subject" counts was created.
        List<String> queryTerms = new ArrayList<>();
        queryTerms = new ArrayList<>();
        queryTerms.add("urn:gem:etype#1234");
        final List<IndexEntry> subjectEntries = service.query(
                null,
                ProspectorConstants.COUNT,
                TripleValueType.SUBJECT.getIndexType(),
                queryTerms,
                XMLSchema.ANYURI.stringValue(),
                auths);
        assertEquals(subjectEntries.size(), 1);
    }

    @Test
    public void testSimpleScenario() throws Exception {
        // Load some data into Accumulo
        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata1"))); //2
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata2"))); //3
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("12"))); //1
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12"))); //4
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred1"), new RyaType("12"))); //5

        ryaDAO.destroy();

        SortedSet<Text> splits = new TreeSet<>();
        splits.add(new Text("urn:gem:etype#1234"));
        connector.tableOperations().addSplits(IN_TABLE, splits);

        // Run the pipeline
        StatisticsPipelineOptions options = PipelineOptionsFactory
                .as(StatisticsPipelineOptions.class);

        options.setAccumuloName(accumulo.getInstanceName());
        options.setZookeeperServers(accumulo.getZooKeepers());
        options.setAccumuloUsername(ACCUMULO_USERNAME);
        options.setAccumuloPassword(ACCUMULO_PASSWORD);
        options.setSource(IN_TABLE);
        options.setDestination(OUT_TABLE);
        options.setBatchSize(10);

        StatisticsPipeline
                .create(options)
                .run()
                .waitUntilFinish();

        final ProspectorService service = new ProspectorService(connector, OUT_TABLE);
        final String[] auths = {"U", "FOUO"};

        // Ensure one of the correct "entity" counts was created.
        List<String> queryTerms = new ArrayList<>();
        queryTerms.add("urn:gem:etype");
        final List<IndexEntry> entityEntries = service.query(null,
                ProspectorConstants.COUNT,
                TripleValueType.ENTITY.getIndexType(),
                queryTerms,
                IndexWorkPlan.URITYPE,
                auths);
        assertEquals(entityEntries.size(), 1);

        final List<IndexEntry> expectedEntityEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("urn:gem:etype")
                        .setDataType(IndexWorkPlan.URITYPE)
                        .setTripleValueType(TripleValueType.ENTITY.getIndexType())
                        .setVisibility("")
                        .setTimestamp(entityEntries.get(0).getTimestamp())
                        .setCount(5L)
                        .build());

        assertEquals(expectedEntityEntries, entityEntries);

        // Ensure one of the correct "subject" counts was created.
        queryTerms = new ArrayList<>();
        queryTerms.add("urn:gem:etype#1234");
        final List<IndexEntry> subjectEntries = service.query(
                null,
                ProspectorConstants.COUNT,
                TripleValueType.SUBJECT.getIndexType(),
                queryTerms,
                XMLSchema.ANYURI.stringValue(),
                auths);
        assertEquals(subjectEntries.size(), 1);

        final List<IndexEntry> expectedSubjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("urn:gem:etype#1234")
                        .setDataType(XMLSchema.ANYURI.stringValue())
                        .setTripleValueType(TripleValueType.SUBJECT.getIndexType())
                        .setVisibility("")
                        .setTimestamp(subjectEntries.get(0).getTimestamp())
                        .setCount(3L)
                        .build());

        assertEquals(expectedSubjectEntries, subjectEntries);

        // Ensure one of the correct "predicate" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem#pred");
        final List<IndexEntry> predicateEntries = service.query(
                null, ProspectorConstants.COUNT,
                TripleValueType.PREDICATE.getIndexType(),
                queryTerms,
                XMLSchema.ANYURI.stringValue(),
                auths);
        assertEquals(predicateEntries.size(), 1);

        final List<IndexEntry> expectedPredicateEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("urn:gem#pred")
                        .setDataType(XMLSchema.ANYURI.stringValue())
                        .setTripleValueType(TripleValueType.PREDICATE.getIndexType())
                        .setVisibility("")
                        .setTimestamp(predicateEntries.get(0).getTimestamp())
                        .setCount(4L)
                        .build());

        assertEquals(expectedPredicateEntries, predicateEntries);

        // Ensure one of the correct "object" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("mydata1");
        final List<IndexEntry> objectEntries = service.query(
                null,
                ProspectorConstants.COUNT,
                TripleValueType.OBJECT.getIndexType(),
                queryTerms,
                XMLSchema.STRING.stringValue(),
                auths);

        assertEquals(objectEntries.size(), 1);

        final List<IndexEntry> expectedObjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("mydata1")
                        .setDataType(XMLSchema.STRING.stringValue())
                        .setTripleValueType(TripleValueType.OBJECT.getIndexType())
                        .setVisibility("")
                        .setTimestamp(objectEntries.get(0).getTimestamp())
                        .setCount(1L)
                        .build());

        assertEquals(expectedObjectEntries, objectEntries);

        // Ensure one of the correct "subjectpredicate" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem:etype#1234");
        queryTerms.add("urn:gem#pred");
        final List<IndexEntry> subjectPredicateEntries = service.query(
                null,
                ProspectorConstants.COUNT,
                TripleValueType.SUBJECT_PREDICATE.getIndexType(),
                queryTerms,
                XMLSchema.STRING.stringValue(),
                auths);

        assertEquals(subjectPredicateEntries.size(), 1);

        final List<IndexEntry> expectedSubjectPredicateEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("urn:gem:etype#1234" + "\u0000" + "urn:gem#pred")
                        .setDataType(XMLSchema.STRING.stringValue())
                        .setTripleValueType(TripleValueType.SUBJECT_PREDICATE.getIndexType())
                        .setVisibility("")
                        .setTimestamp(subjectPredicateEntries.get(0).getTimestamp())
                        .setCount(3L)
                        .build());

        assertEquals(expectedSubjectPredicateEntries, subjectPredicateEntries);

        // Ensure one of the correct "predicateobject" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem#pred");
        queryTerms.add("12");
        final List<IndexEntry> predicateObjectEntries = service.query(
                null,
                ProspectorConstants.COUNT,
                TripleValueType.PREDICATE_OBJECT.getIndexType(),
                queryTerms,
                XMLSchema.STRING.stringValue(),
                auths);

        assertEquals(predicateObjectEntries.size(), 1);

        final List<IndexEntry> expectedPredicateObjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("urn:gem#pred" + "\u0000" + "12")
                        .setDataType(XMLSchema.STRING.stringValue())
                        .setTripleValueType(TripleValueType.PREDICATE_OBJECT.getIndexType())
                        .setVisibility("")
                        .setTimestamp(predicateObjectEntries.get(0).getTimestamp())
                        .setCount(2L) // XXX This might be a bug. The object matching doesn't care about type.
                        .build());

        assertEquals(expectedPredicateObjectEntries, predicateObjectEntries);

        // Ensure one of the correct "subjectobject" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem:etype#1234");
        queryTerms.add("mydata1");
        final List<IndexEntry> subjectObjectEntries = service.query(
                null,
                ProspectorConstants.COUNT,
                TripleValueType.SUBJECT_OBJECT.getIndexType(),
                queryTerms,
                XMLSchema.STRING.stringValue(),
                auths);

        assertEquals(subjectObjectEntries.size(), 1);

        final List<IndexEntry> expectedSubjectObjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                        .setIndex(ProspectorConstants.COUNT)
                        .setData("urn:gem:etype#1234" + "\u0000" + "mydata1")
                        .setDataType(XMLSchema.STRING.stringValue())
                        .setTripleValueType(TripleValueType.SUBJECT_OBJECT.getIndexType())
                        .setVisibility("")
                        .setTimestamp(subjectObjectEntries.get(0).getTimestamp())
                        .setCount(1L)
                        .build());

        assertEquals(expectedSubjectObjectEntries, subjectObjectEntries);
    }

    @Test
    public void testWithEvalStatsDAOCount() throws Exception {
        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata1")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata2")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("12")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred1"), new RyaType("12")));

        ryaDAO.destroy();

        // Run the pipeline
        StatisticsPipelineOptions options = PipelineOptionsFactory
                .as(StatisticsPipelineOptions.class);

        options.setAccumuloName(accumulo.getInstanceName());
        options.setZookeeperServers(accumulo.getZooKeepers());
        options.setAccumuloUsername(ACCUMULO_USERNAME);
        options.setAccumuloPassword(ACCUMULO_PASSWORD);
        options.setSource(IN_TABLE);
        options.setDestination(OUT_TABLE);
        options.setBatchSize(2);

        StatisticsPipeline
                .create(options)
                .run()
                .waitUntilFinish();

        final AccumuloRdfConfiguration rdfConf = new AccumuloRdfConfiguration();
        rdfConf.setAuths("U", "FOUO");

        final ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(connector, rdfConf);
        evalDao.init();

        // Get the cardinality of the 'urn:gem#pred' predicate.
        List<Value> values = new ArrayList<Value>();
        values.add(VF.createIRI("urn:gem#pred"));
        double count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE, values);
        assertEquals(4.0, count, 0.001);

        // Get the cardinality of the 'mydata1' object.
        values = new ArrayList<Value>();
        values.add(VF.createLiteral("mydata1"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(1.0, count, 0.001);

        // Get the cardinality of the 'mydata3' object.
        values = new ArrayList<Value>();
        values.add(VF.createLiteral("mydata3"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(-1.0, count, 0.001);
    }

}
