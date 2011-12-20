package edu.brown.lasvegas.lvfs.meta;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import edu.brown.lasvegas.protocol.MetadataProtocol;
import edu.brown.lasvegas.server.CentralNode;

/**
 * Testcase for RPC version of {@link MetadataProtocol}.
 */
public class CentralNodeMetadataRepositoryTest extends MetadataRepositoryTestBase {
    private static final String METAREPO_ADDRESS = "localhost:18710"; // use a port different from the default.
    private static final String METAREPO_BDBHOME = "test/metatest";

    private static CentralNode centralNode;
    private static MetadataProtocol metaClient;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        File bdbFolder = new File(METAREPO_BDBHOME);
        if (bdbFolder.exists()) {
            File backup = new File(bdbFolder.getParentFile(), bdbFolder.getName() + "_backup_"
                        + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) // append backup-date
                        + "_" + new Random(System.nanoTime()).nextInt()); // to make it unique
            boolean renamed = bdbFolder.renameTo(backup);
            if (!renamed) {
                throw new IOException ("failed to take a backup of existing testing-bdbhome");
            }
        }
        Configuration conf = new Configuration();
        conf.set(CentralNode.METAREPO_ADDRESS_KEY, METAREPO_ADDRESS);
        conf.set(CentralNode.METAREPO_BDBHOME_KEY, METAREPO_BDBHOME);
        centralNode = CentralNode.createInstance(conf);
        metaClient = RPC.getProxy(MetadataProtocol.class, MetadataProtocol.versionID, NetUtils.createSocketAddr(METAREPO_ADDRESS), conf);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        centralNode.stop();
        centralNode.join();
        centralNode = null;
    }

    @Before
    public void setUp() throws Exception {
        super.baseSetUp(metaClient);
    }

    @After
    public void tearDown() throws Exception {
        super.baseTearDown();
    }
    
    @Override
    protected void reloadRepository() throws IOException {
        // only sync(). disk durability is already tested in the Master version of the test.
        // this testcase focuses on server-client communication.
        metaClient.sync();
    }
}
