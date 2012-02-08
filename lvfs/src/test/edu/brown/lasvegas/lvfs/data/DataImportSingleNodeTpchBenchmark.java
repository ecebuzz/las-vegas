package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
 * TPCH version. This co-partitions lineitem and part.
 * This is NOT a testcase.
 */
public class DataImportSingleNodeTpchBenchmark {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE_ADDRESS = "localhost:12345";
    private static final String DATANODE_NAME = "node";
    private static final Logger LOG = Logger.getLogger(DataImportSingleNodeTpchBenchmark.class);

    private static final String lvfsRoot = "test";
    // private static final File inputFile = new File ("../tpch-dbgen/lineorder_s1.tbl");
    private static final File lineitemFile = new File ("../tpch-dbgen/lineitem.tbl");
    private static final File partFile = new File ("../tpch-dbgen/part.tbl");
    private static final int partitionCount = 2;

    /*
    // just for testing
    private static final File lineitemFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl");
    private static final File partFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_part.tbl");
    private static final int partitionCount = 1;
    */

    private MasterMetadataRepository masterRepository;
    private String rootDir;
    private String tmpDir;
    private LVDataNode dataNode;
    private Configuration conf;

    private LVRack rack;
    private LVRackNode node;
    private LVDatabase database;
    private LVTable lineitemTable, partTable;
    private LVReplicaGroup lineitemGroup, partGroup;

    private final MiniDataSource lineitemSource = new MiniTPCHLineitem();
    private final MiniDataSource partSource = new MiniTPCHPart();

    private int[] getColumnIds (LVTable table) throws IOException {
        LVColumn[] columns = masterRepository.getAllColumnsExceptEpochColumn(table.getTableId());
        int[] columnIds = new int[columns.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = columns[i].getColumnId();
        }
        return columnIds;
    }

    private void setUp () throws IOException {
        ClearAllTest.deleteFileRecursive(new File("test"));
        if (!lineitemFile.exists()) {
            throw new FileNotFoundException(lineitemFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        if (!partFile.exists()) {
            throw new FileNotFoundException(partFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        LOG.info("partitions the data into " + partitionCount + " partitions");

        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        rack = masterRepository.createNewRack("rack");
        node = masterRepository.createNewRackNode(rack, DATANODE_NAME, DATANODE_ADDRESS);
        database = masterRepository.createNewDatabase("db1");
        
        ValueRange[] ranges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
            ranges[i] = new ValueRange ();
            ranges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
                ranges[i].setStartKey(null);
            } else {
                ranges[i].setStartKey(200000 * i + 1);
            }
            if (i == partitionCount - 1) {
                ranges[i].setEndKey(null);
            } else {
                ranges[i].setEndKey(200000 * (i + 1) + 1);
            }
        }

        partTable = masterRepository.createNewTable(database.getDatabaseId(), "part", partSource.getColumnNames(), partSource.getScheme());
        partGroup = masterRepository.createNewReplicaGroup(partTable, masterRepository.getColumnByName(partTable.getTableId(), "p_partkey"), ranges);
        masterRepository.createNewReplicaScheme(partGroup, masterRepository.getColumnByName(partTable.getTableId(), "p_partkey"), getColumnIds(partTable), partSource.getDefaultCompressions());
        
        lineitemTable = masterRepository.createNewTable(database.getDatabaseId(), "lineitem", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroup = masterRepository.createNewReplicaGroup(lineitemTable, masterRepository.getColumnByName(lineitemTable.getTableId(), "l_partkey"), partGroup); // link to partGroup
        masterRepository.createNewReplicaScheme(lineitemGroup, masterRepository.getColumnByName(lineitemTable.getTableId(), "l_partkey"), getColumnIds(lineitemTable), lineitemSource.getDefaultCompressions());

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
        loadTable (partTable, partFile);
        loadTable (lineitemTable, lineitemFile);
    }
    private void loadTable (LVTable table, File inputFile) throws Exception {
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node.getNodeId(), new String[]{inputFile.getAbsolutePath()});
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
        DataImportSingleNodeTpchBenchmark program = new DataImportSingleNodeTpchBenchmark();
        program.setUp();
        try {
            LOG.info("started:" + lineitemFile.getName() + " and " + partFile.getName());
            long start = System.currentTimeMillis();
            program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended:" + lineitemFile.getName() + " and " + partFile.getName() + ". elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }
}
