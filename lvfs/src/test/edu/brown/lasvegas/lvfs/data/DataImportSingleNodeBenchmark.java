package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ClearAllTest;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;
import edu.brown.lasvegas.util.ValueRange;

/**
 * A performance benchmark program to test large data import
 * in a single node.
 * This is NOT a testcase.
 * 
 * Because it's a single node and we don't actually launch
 * HDFS instances, this one is much easier to run.
 * @see DataImportMultiNodeBenchmark
 */
public class DataImportSingleNodeBenchmark {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE_ADDRESS = "localhost:12345";
    private static final String DATANODE_NAME = "node";
    private static final Logger LOG = Logger.getLogger(DataImportSingleNodeBenchmark.class);

    // private static final String lvfsRoot = "/tmp/test";
    // private static final File inputFile = new File ("/tmp/lineorder_s1.tbl");

    private static final String lvfsRoot = "test";
    private static final File inputFile = new File ("../ssb-dbgen/lineorder_s1.tbl");
    // private static final File inputFile = new File ("../ssb-dbgen/lineorder_s4.tbl");
    // private static final File inputFile = new File ("../ssb-dbgen/lineorder_s15.tbl");
    // private static final File inputFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_lineorder.tbl"); // just for testing

    private MasterMetadataRepository masterRepository;
    private String rootDir;
    private String tmpDir;
    private LVDataNode dataNode;
    private Configuration conf;

    private LVRack rack;
    private LVRackNode node;
    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;

    private final MiniDataSource dataSource = new MiniSSBLineorder();

    private void setUp () throws IOException {
        ClearAllTest.deleteFileRecursive(new File("test"));
        if (!inputFile.exists()) {
            throw new FileNotFoundException(inputFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        LOG.info("input file size:" + (inputFile.length() >> 20) + "MB");
        final int BYTES_PER_TUPLE = 100; // well, largely.
        final long TOTAL_TUPLES = inputFile.length() / BYTES_PER_TUPLE;
        int partitionCount = (int) Math.round((double) TOTAL_TUPLES / 6000000.0d);
        if (partitionCount == 0) {
            partitionCount = 1;
        }
        LOG.info("partitions the data into " + partitionCount + " partitions");

        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        rack = masterRepository.createNewRack("rack");
        node = masterRepository.createNewRackNode(rack, DATANODE_NAME, DATANODE_ADDRESS);
        database = masterRepository.createNewDatabase("db1");
        final String[] columnNames = dataSource.getColumnNames();
        columns = new HashMap<String, LVColumn>();
        table = masterRepository.createNewTable(database.getDatabaseId(), "lineorder", columnNames, dataSource.getScheme());
        for (LVColumn column : masterRepository.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }
        columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        ValueRange[] ranges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
            ranges[i] = new ValueRange ();
            ranges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
                ranges[i].setStartKey(null);
            } else {
                ranges[i].setStartKey(6000000 * i + 1);
            }
            if (i == partitionCount - 1) {
                ranges[i].setEndKey(null);
            } else {
                ranges[i].setEndKey(6000000 * (i + 1) + 1);
            }
        }
        group = masterRepository.createNewReplicaGroup(table, columns.get("lo_orderkey"), ranges);
        masterRepository.createNewReplicaScheme(group, columns.get("lo_orderkey"), columnIds, dataSource.getDefaultCompressions());
        masterRepository.createNewReplicaScheme(group, columns.get("lo_suppkey"), columnIds, dataSource.getDefaultCompressions());
        masterRepository.createNewReplicaScheme(group, columns.get("lo_orderdate"), columnIds, dataSource.getDefaultCompressions());

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
    }
    
    private void tearDown () throws IOException {
        dataNode.close();
        masterRepository.shutdown();
    }
    
    public void exec () throws Exception {
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.addNodeFilePath(node.getNodeId(), inputFile.getAbsolutePath());
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 1000L, 1000L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : masterRepository.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        DataImportSingleNodeBenchmark program = new DataImportSingleNodeBenchmark();
        program.setUp();
        try {
            LOG.info("started:" + inputFile.getName());
            long start = System.currentTimeMillis();
            program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended:" + inputFile.getName() + ". elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }
}
