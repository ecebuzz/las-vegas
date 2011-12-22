package edu.brown.lasvegas.lvfs.meta;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Testcase for RPC version of {@link LVMetadataProtocol}.
 */
public class CentralNodeMetadataRepositoryTest extends MetadataRepositoryTestBase {
    private static final String METAREPO_ADDRESS = "localhost:18710"; // use a port different from the default.
    private static final String METAREPO_BDBHOME = "test/metatest";

    private static LVCentralNode centralNode;
    private static LVMetadataProtocol metaClient;
    private static MiniDFSCluster dfsCluster;
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
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, METAREPO_ADDRESS);
        conf.set(LVCentralNode.METAREPO_BDBHOME_KEY, METAREPO_BDBHOME);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY, 10000); // to speed-up testing
        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
            .format(true).racks(null).build();
        centralNode = new LVCentralNode(conf);
        centralNode.start(dfsCluster.getNameNode()); 
        metaClient = RPC.getProxy(LVMetadataProtocol.class, LVMetadataProtocol.versionID, NetUtils.createSocketAddr(METAREPO_ADDRESS), conf);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        centralNode.stop();
        centralNode.join();
        centralNode = null;
        dfsCluster.shutdown();
        dfsCluster = null;
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
