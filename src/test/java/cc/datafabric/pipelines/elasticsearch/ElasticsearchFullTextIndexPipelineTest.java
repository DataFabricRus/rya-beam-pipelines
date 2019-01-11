package cc.datafabric.pipelines.elasticsearch;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.persist.RyaDAOException;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticsearchFullTextIndexPipelineTest {

    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "pass";
    private static final String IN_TABLE = "rya_spo";

    private MiniAccumuloCluster accumulo;
    private File tempDir;
    private Connector connector;

    private EmbeddedElastic embeddedElastic;

    @Before
    public void before() throws Exception {
        tempDir = new File(FileUtils.getTempDirectory(), "accumulo-" + System.currentTimeMillis());
        assert tempDir.mkdirs();

        accumulo = new MiniAccumuloCluster(tempDir, ACCUMULO_PASSWORD);
        accumulo.start();

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        connector = instance.getConnector(ACCUMULO_USERNAME, new PasswordToken(ACCUMULO_PASSWORD));
        connector.securityOperations().changeUserAuthorizations(ACCUMULO_USERNAME, new Authorizations("U", "FOUO"));

        embeddedElastic = EmbeddedElastic.builder()
                .withElasticVersion("5.6.9")
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9300)
                .withSetting(PopularProperties.CLUSTER_NAME, "elasticsearch")
                .withStartTimeout(60, TimeUnit.SECONDS)
                .build()
                .start();
    }

    @After
    public void after() throws IOException, InterruptedException {
        accumulo.stop();
        FileUtils.forceDelete(tempDir);

        embeddedElastic.stop();
    }

    @Test
    public void testWithoutTimestampFilter() throws Exception {
        // Load some data into Accumulo
        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"),
                new RyaType(XMLSchema.STRING, "mydata1", "en"))); //2
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata2"))); //3
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("12"))); //1
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12"))); //4
        ryaDAO.add(new RyaStatement(
                new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred1"), new RyaType("12"))); //5

        ryaDAO.destroy();

        // Run the pipeline
        ElasticsearchFullTextIndexPipelineOptions options = PipelineOptionsFactory
                .as(ElasticsearchFullTextIndexPipelineOptions.class);

        options.setAccumuloName(accumulo.getInstanceName());
        options.setZookeeperServers(accumulo.getZooKeepers());
        options.setAccumuloUsername(ACCUMULO_USERNAME);
        options.setAccumuloPassword(ACCUMULO_PASSWORD);
        options.setSource(IN_TABLE);
        options.setElasticsearchHost("localhost");
        options.setProperties(new String[]{
                "urn:gem#pred"
        });
        options.setBatchSize(2);

        ElasticsearchFullTextIndexPipeline
                .create(options)
                .run()
                .waitUntilFinish();

        List<String> documents = embeddedElastic.fetchAllDocuments("elastic-search-sail");

        assertTrue(!documents.isEmpty());

        System.out.println(documents);
    }

    @Test
    public void testWithTimestampFilter() throws RyaDAOException, UnknownHostException {
        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        long newTimestamp = Instant.now().getEpochSecond() * 1000;
        long oldTimestamp = Instant.now().minusSeconds(60000).getEpochSecond() * 1000;

        ryaDAO.add(RyaStatement.builder()
                .setSubject(new RyaIRI("urn:gem:etype#1234"))
                .setPredicate(new RyaIRI("urn:gem#pred"))
                .setObject(new RyaType(XMLSchema.STRING, "mydata1", "en"))
                .setTimestamp(oldTimestamp)
                .build());
        ryaDAO.add(RyaStatement.builder()
                .setSubject(new RyaIRI("urn:gem:etype#1234"))
                .setPredicate(new RyaIRI("urn:gem#pred"))
                .setObject(new RyaType("mydata2"))
                .setTimestamp(oldTimestamp)
                .build());
        ryaDAO.add(RyaStatement.builder()
                .setSubject(new RyaIRI("urn:gem:etype#1235"))
                .setPredicate(new RyaIRI("urn:gem#pred"))
                .setObject(new RyaType(XMLSchema.INTEGER, "12"))
                .setTimestamp(oldTimestamp)
                .build());
        ryaDAO.add(RyaStatement.builder()
                .setSubject(new RyaIRI("urn:gem:etype#1235"))
                .setPredicate(new RyaIRI("urn:gem#pred1"))
                .setObject(new RyaType("12"))
                .setTimestamp(oldTimestamp)
                .build());

        ryaDAO.add(RyaStatement.builder()
                .setSubject(new RyaIRI("urn:gem:etype#1234"))
                .setPredicate(new RyaIRI("urn:gem#pred"))
                .setObject(new RyaType("12"))
                .setTimestamp(newTimestamp)
                .build());

        ryaDAO.destroy();

        // Run the pipeline
        ElasticsearchFullTextIndexPipelineOptions options = PipelineOptionsFactory
                .as(ElasticsearchFullTextIndexPipelineOptions.class);

        options.setAccumuloName(accumulo.getInstanceName());
        options.setZookeeperServers(accumulo.getZooKeepers());
        options.setAccumuloUsername(ACCUMULO_USERNAME);
        options.setAccumuloPassword(ACCUMULO_PASSWORD);
        options.setSource(IN_TABLE);
        options.setElasticsearchHost("localhost");
        options.setProperties(new String[]{
                "urn:gem#pred"
        });
        options.setBatchSize(2);
        options.setStartDateTime(DateTimeFormatter.ISO_DATE_TIME
                .format(ZonedDateTime.now().minusSeconds(10)));
        options.setEndDateTime(DateTimeFormatter.ISO_DATE_TIME
                .format(ZonedDateTime.now()));

        ElasticsearchFullTextIndexPipeline
                .createWithTimestampFilter(options)
                .run()
                .waitUntilFinish();

        List<String> documents = embeddedElastic.fetchAllDocuments("elastic-search-sail");

        System.out.println(documents);

        assertTrue(documents.size() == 1);
        assertEquals(
                "{\"context\":\"null\",\"text\":[\"12\",\"mydata1\",\"mydata2\"],\"uri\":\"urn:gem:etype#1234\",\"p_urn:gem#pred\":[\"12\",\"mydata1\",\"mydata2\"]}",
                documents.get(0)
        );
    }
}
