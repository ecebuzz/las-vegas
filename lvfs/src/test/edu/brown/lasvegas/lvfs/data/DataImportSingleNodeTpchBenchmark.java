package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ClearAllTest;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * TPCH version.
 * This is NOT a testcase.
 */
public class DataImportSingleNodeTpchBenchmark extends DataImportTpchBenchmark {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE_ADDRESS = "localhost:12345";
    private static final String DATANODE_NAME = "node";
    private static final Logger LOG = Logger.getLogger(DataImportSingleNodeTpchBenchmark.class);

    private static final String lvfsRoot = "test";

    private static final File[] lineitemFiles = new File[]{new File ("../tpch-dbgen/s2/lineitem.tbl.1"), new File ("../tpch-dbgen/s2/lineitem.tbl.2")};
    private static final File[] partFiles = new File[]{new File ("../tpch-dbgen/s2/part.tbl.1"), new File ("../tpch-dbgen/s2/part.tbl.2")};
    private static final File[] ordersFiles = new File[]{new File ("../tpch-dbgen/s2/orders.tbl.1"), new File ("../tpch-dbgen/s2/orders.tbl.2")};
    private static final File[] customerFiles = new File[]{new File ("../tpch-dbgen/s2/customer.tbl.1"), new File ("../tpch-dbgen/s2/customer.tbl.2")};
    private static final int partitionCount = 2;
    private static final int fractureCount = 2;
    /*
    private static final File[] lineitemFiles = new File[]{new File ("../tpch-dbgen/lineitem.tbl")};
    private static final File[] partFiles = new File[]{new File ("../tpch-dbgen/part.tbl")};
    private static final File[] ordersFiles = new File[]{new File ("../tpch-dbgen/orders.tbl")};
    private static final File[] customerFiles = new File[]{new File ("../tpch-dbgen/customer.tbl")};
    private static final int partitionCount = 2;
    private static final int fractureCount = 2;
    */
    
/*
    // just for testing
    private static final File[] lineitemFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl")};
    private static final File[] partFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_part.tbl")};
    private static final File[] ordersFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_orders.tbl")};
    private static final File[] customerFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_customer.tbl")};
    private static final int partitionCount = 1;
*/
    private MasterMetadataRepository masterRepository;
    private String rootDir;
    private String tmpDir;
    private LVDataNode dataNode;
    private Configuration conf;

    private LVRack rack;
    private LVRackNode node;

    DataImportSingleNodeTpchBenchmark (MasterMetadataRepository masterRepository) throws IOException {
        super(masterRepository, partitionCount, fractureCount);
        this.masterRepository = masterRepository;
    }
    static DataImportSingleNodeTpchBenchmark getInstance () throws IOException {
        ClearAllTest.deleteFileRecursive(new File("test"));
        for (File[] files : new File[][]{lineitemFiles, partFiles, ordersFiles, customerFiles}) {
            for (File file : files) {
    	        if (!file.exists()) {
    	            throw new FileNotFoundException(file.getAbsolutePath() + " doesn't exist. Have you generated the data?");
    	        }
            }
        }

        MasterMetadataRepository masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        return new DataImportSingleNodeTpchBenchmark(masterRepository);
    }
    @Override
    public void setUp() throws IOException {
        super.setUp();
        rack = masterRepository.createNewRack("rack");
        node = masterRepository.createNewRackNode(rack, DATANODE_NAME, DATANODE_ADDRESS);
        
        conf = new Configuration();
        rootDir = lvfsRoot + "/node_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
        tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCAL_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCAL_LVFS_TMPDIR_KEY, tmpDir);
        conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
        conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 1000L);
        conf.set(LVDataNode.DATA_ADDRESS_KEY, DATANODE_ADDRESS);
        conf.set(LVDataNode.DATA_NODE_NAME_KEY, DATANODE_NAME);
        conf.set(LVDataNode.DATA_RACK_NAME_KEY, "rack");
        dataNode = new LVDataNode(conf, masterRepository);
        dataNode.start(null);
    }
    protected void tearDown () throws IOException {
        dataNode.close();
        masterRepository.shutdown();
    }
    void exec () throws Exception {
        InputFile lineitemInputFile = new InputFile("lineitem", lineitemFiles);
        InputFile partInputFile = new InputFile("part", partFiles);
        InputFile customerInputFile = new InputFile("customer", customerFiles);
        InputFile ordersInputFile = new InputFile("orders", ordersFiles);
        super.exec(lineitemInputFile.path(), partInputFile.path(), customerInputFile.path(), ordersInputFile.path());
        lineitemInputFile.delete();
        partInputFile.delete();
        customerInputFile.delete();
        ordersInputFile.delete();
    }
    private class InputFile {
        InputFile (String name, File[] files) throws IOException {
            this.inputFile = new File (lvfsRoot, name + "_list.txt");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(inputFile), "UTF-8"));
            for (File file : files) {
                writer.write(node.getName() + "\t" + file.getAbsolutePath());
                writer.newLine();
            }
            writer.flush();
            writer.close();
        }
        void delete () throws IOException {
            if (inputFile.exists()) {
                inputFile.delete();
            }
        }
        String path () { return inputFile.getAbsolutePath(); }
        @Override
        protected void finalize() throws Throwable {
            delete ();
        }
        private final File inputFile;
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        DataImportSingleNodeTpchBenchmark program = DataImportSingleNodeTpchBenchmark.getInstance();
        program.setUp();
        program.exec();
    }
}
