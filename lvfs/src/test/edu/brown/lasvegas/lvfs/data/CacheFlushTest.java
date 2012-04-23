package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Testcase for {@link CacheFlusher}.
 */
public class CacheFlushTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE_ADDRESS = "localhost:12345";
    private static final String DATANODE_NAME = "node";

    private static final String lvfsRoot = "test";
    
    private static final File file = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl");
    // private static final File file = new File ("../tpch-dbgen/lineitem.tbl");
    
    private MasterMetadataRepository masterRepository;
    private String rootDir;
    private String tmpDir;
    private LVDataNode dataNode;
    private Configuration conf;

    private LVRack rack;
    private LVRackNode node;
    private CacheFlusher flusher;

    @Before
    public void setUp () throws IOException {
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        rack = masterRepository.createNewRack("rack");
        node = masterRepository.createNewRackNode(rack, DATANODE_NAME, DATANODE_ADDRESS);
        
        conf = new Configuration();
        rootDir = lvfsRoot + "/node_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
        tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
        conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
        conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 1000L);
        conf.set(LVDataNode.DATA_ADDRESS_KEY, DATANODE_ADDRESS);
        conf.set(LVDataNode.DATA_NODE_NAME_KEY, DATANODE_NAME);
        conf.set(LVDataNode.DATA_RACK_NAME_KEY, "rack");
        dataNode = new LVDataNode(conf, masterRepository);
        dataNode.start(null);
        flusher = new CacheFlusher(masterRepository);
    }

    @After
    public void tearDown () throws IOException {
    	flusher.tearDown();
        dataNode.close();
        masterRepository.shutdown();
    }
    
    @Test
    public void exec () throws Exception {
    	File inputFile = new File (lvfsRoot, "input_" + Math.abs(new Random(System.nanoTime()).nextInt()) + ".txt");
    	FileWriter writer = new FileWriter(inputFile);
    	writer.write(node.getName() + "\t" + file.getAbsolutePath());
    	writer.flush();
    	writer.close();
    	flusher.exec(inputFile.getAbsolutePath());
    }
}
