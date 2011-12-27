package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.server.LVCentralNode;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Testcases for {@link DataEngine} with RPC.
 */
public class RemoteDataEngineTest extends DataEngineTestBase {
    private static final String METAREPO_ADDRESS = "localhost:18710"; // use a port different from the default.
    private static final String METAREPO_BDBHOME = "test/metatest";
    private static final String DATA_ADDRESS = "localhost:18712";

    private static LVCentralNode centralNode;
    private static LVDataNode dataNode;
    private static LVMetadataClient metaClient;
    private static LVDataClient dataClient;
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
        conf.set(LVDataNode.DATA_ADDRESS_KEY, DATA_ADDRESS);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY, 10000); // to speed-up testing
        String rootDir = "test/node2_lvfs_" + new Random(System.nanoTime()).nextInt();
        String tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
        final String NODE_NAME = "node2";
        final String RACK_NAME = "rack1";
        conf.set(LVDataNode.DATA_NODE_NAME_KEY, NODE_NAME);
        conf.set(LVDataNode.DATA_RACK_NAME_KEY, RACK_NAME);

        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
            .format(true).racks(null).build();
        centralNode = new LVCentralNode(conf);
        centralNode.start(dfsCluster.getNameNode()); 
        metaClient = new LVMetadataClient(conf);

        LVRack rack = metaClient.getChannel().createNewRack(RACK_NAME);
        LVRackNode node = metaClient.getChannel().createNewRackNode(rack, NODE_NAME);
        assertTrue (node != null);
        dataNode = new LVDataNode(conf);
        dataNode.start(dfsCluster.getDataNodes().get(0));
        dataClient = new LVDataClient(conf, DATA_ADDRESS);

        dataProtocol = dataClient.getChannel();
        setDataNodeDirs(rootDir, tmpDir);
        createRandomFiles();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        metaClient.release();
        dataClient.release();
        centralNode.stop();
        centralNode.join();
        centralNode = null;
        dataNode.stop();
        dataNode.join();
        dataNode = null;
        dfsCluster.shutdown();
        dfsCluster = null;
    }
}
