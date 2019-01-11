package cc.datafabric.pipelines.bulkload;

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
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BulkLoadPipelineTest {

    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "pass";
    private static final String DEST_TABLE_PREFIX = "rya_";

    private MiniAccumuloCluster accumulo;
    private File tempDir;
    private Connector connector;

    @Before
    public void before() throws Exception {
        tempDir = new File(FileUtils.getTempDirectory(), "accumulo-" + System.currentTimeMillis());
        assert tempDir.mkdirs();

        accumulo = new MiniAccumuloCluster(tempDir, ACCUMULO_PASSWORD);
        accumulo.start();

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        connector = instance.getConnector(ACCUMULO_USERNAME, new PasswordToken(ACCUMULO_PASSWORD));
        connector.securityOperations().changeUserAuthorizations(ACCUMULO_USERNAME, new Authorizations("U", "FOUO"));
    }

    @After
    public void after() throws IOException, InterruptedException {
        accumulo.stop();
        FileUtils.forceDelete(tempDir);
    }

    @Test
    public void test() throws Exception {
        // Run the pipeline
        BulkLoadPipelineOptions options = PipelineOptionsFactory
                .as(BulkLoadPipelineOptions.class);

        options.setAccumuloName(accumulo.getInstanceName());
        options.setZookeeperServers(accumulo.getZooKeepers());
        options.setAccumuloUsername(ACCUMULO_USERNAME);
        options.setAccumuloPassword(ACCUMULO_PASSWORD);

        String folder = this.getClass().getResource("test-1").toString();

        options.setSource(folder + "/*.nt," + folder + "/folder/*.ttl");

        options.setDestinationTablePrefix(DEST_TABLE_PREFIX);
        options.setBatchSize(2);

        BulkLoadPipeline
                .create(options)
                .run()
                .waitUntilFinish();

        // Check that the data was loaded
        final AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(connector);
        dao.init();

        assertTripleExists(dao,
                "urn:a", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "urn:class:1");
        assertTripleExists(dao,
                "urn:a", "http://www.w3.org/2000/01/rdf-schema#label", "a");
        assertTripleExists(dao,
                "urn:a", "urn:p:related", "urn:b");
        assertTripleExists(dao,
                "urn:b", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "urn:class:1");
        assertTripleExists(dao,
                "urn:b", "http://www.w3.org/2000/01/rdf-schema#label", "b");
        assertTripleExists(dao,
                "urn:b", "urn:p:related", "urn:a");

        dao.destroy();
    }

    private void assertTripleExists(AccumuloRyaDAO dao, String subject, String predicate, String object) {
        if (object.startsWith("http://") || object.startsWith("https://") || object.startsWith("urn:")) {
            assertTripleExists(dao, new RyaIRI(subject), new RyaIRI(predicate), new RyaIRI(object));
        } else {
            assertTripleExists(dao, new RyaIRI(subject), new RyaIRI(predicate), new RyaType(object));
        }
    }

    private void assertTripleExists(AccumuloRyaDAO dao, RyaIRI subject, RyaIRI predicate, RyaType object) {
        try {
            RyaStatement stmt = new RyaStatement.RyaStatementBuilder()
                    .setSubject(subject)
                    .setPredicate(predicate)
                    .setObject(object)
                    .build();

            CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(stmt, null);
            if (iter.hasNext()) {
                RyaStatement found = iter.next();

                assertEquals(subject, found.getSubject());
                assertEquals(predicate, found.getPredicate());
                assertEquals(object, found.getObject());
            } else {
                fail("Triple: [" + subject + ", " + predicate + ", " + object + "] doesn't exist!");
            }
        } catch (RyaDAOException e) {
            throw new IllegalStateException(e);
        }
    }

}
