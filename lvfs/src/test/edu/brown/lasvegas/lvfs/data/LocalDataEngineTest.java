package edu.brown.lasvegas.lvfs.data;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;

/**
 * Testcases for {@link DataEngine} without RPC.
 */
public class LocalDataEngineTest extends DataEngineTestBase {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static MasterMetadataRepository masterRepository;
    private static DataEngine dataEngine;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        LVRack rack = masterRepository.createNewRack("rack1");
        LVRackNode node = masterRepository.createNewRackNode(rack, "node1", "node1:12345");
        Configuration conf = new Configuration();
        String rootDir = "test/node1_lvfs_" + new Random(System.nanoTime()).nextInt();
        String tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCAL_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCAL_LVFS_TMPDIR_KEY, tmpDir);
        dataEngine = new DataEngine(masterRepository, node.getNodeId(), conf);
        dataProtocol = dataEngine;
        setDataNodeDirs(rootDir, tmpDir);
        createRandomFiles();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        dataEngine.shutdown();
        masterRepository.shutdown();
    }
}
