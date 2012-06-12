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
    private static final File ordersFile = new File ("../tpch-dbgen/orders.tbl");
    private static final File customerFile = new File ("../tpch-dbgen/customer.tbl");
    private static final int partitionCount = 2;
    
/*
    // just for testing
    private static final File lineitemFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl");
    private static final File partFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_part.tbl");
    private static final File ordersFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_orders.tbl");
    private static final File customerFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_customer.tbl");
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
    /**
     * a dirty hack for experiments.
     * import two versions of LINEITEM table for two different partitioning.
     * we can just use replica groups for this purpose, but then we can't utilize the limited
     * number of nodes because replica groups in the same fracture must have non-overlapping dedicated racks.
     * If we have 2x more machines in our lab, we don't need this hack.
     */
    private LVTable lineitemTablePart, partTable, lineitemTableOrders, ordersTable, customerTable;
    private LVReplicaGroup lineitemGroupPart, partGroup, lineitemGroupOrders, ordersGroup, customerGroup;

    private final MiniDataSource lineitemSource = new MiniTPCHLineitem();
    private final MiniDataSource partSource = new MiniTPCHPart();
    private final MiniDataSource ordersSource = new MiniTPCHOrders();
    private final MiniDataSource customerSource = new MiniTPCHCustomer();

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
        for (File file : new File[]{lineitemFile, partFile, ordersFile, customerFile}) {
	        if (!file.exists()) {
	            throw new FileNotFoundException(file.getAbsolutePath() + " doesn't exist. Have you generated the data?");
	        }
        }
        LOG.info("partitions the data into " + partitionCount + " partitions");

        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        rack = masterRepository.createNewRack("rack");
        node = masterRepository.createNewRackNode(rack, DATANODE_NAME, DATANODE_ADDRESS);
        database = masterRepository.createNewDatabase("db1");
        
        ValueRange[] customerRanges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
        	customerRanges[i] = new ValueRange ();
        	customerRanges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
            	customerRanges[i].setStartKey(null);
            } else {
            	customerRanges[i].setStartKey(150000 * i + 1);
            }
            if (i == partitionCount - 1) {
            	customerRanges[i].setEndKey(null);
            } else {
            	customerRanges[i].setEndKey(150000 * (i + 1) + 1);
            }
        }
        ValueRange[] partRanges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
            partRanges[i] = new ValueRange ();
            partRanges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
                partRanges[i].setStartKey(null);
            } else {
                partRanges[i].setStartKey(200000 * i + 1);
            }
            if (i == partitionCount - 1) {
                partRanges[i].setEndKey(null);
            } else {
                partRanges[i].setEndKey(200000 * (i + 1) + 1);
            }
        }
        ValueRange[] ordersRanges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
        	ordersRanges[i] = new ValueRange ();
        	ordersRanges[i].setType(ColumnType.BIGINT);
            if (i == 0) {
            	ordersRanges[i].setStartKey(null);
            } else {
            	ordersRanges[i].setStartKey(6000000L * i + 1L);
            }
            if (i == partitionCount - 1) {
            	ordersRanges[i].setEndKey(null);
            } else {
            	ordersRanges[i].setEndKey(6000000L * (i + 1) + 1L);
            }
        }
        
        customerTable = masterRepository.createNewTable(database.getDatabaseId(), "customer", customerSource.getColumnNames(), customerSource.getScheme());
        customerGroup = masterRepository.createNewReplicaGroup(customerTable, masterRepository.getColumnByName(customerTable.getTableId(), "c_custkey"), customerRanges);
        masterRepository.createNewReplicaScheme(customerGroup, masterRepository.getColumnByName(customerTable.getTableId(), "c_custkey"), getColumnIds(customerTable), customerSource.getDefaultCompressions());

        partTable = masterRepository.createNewTable(database.getDatabaseId(), "part", partSource.getColumnNames(), partSource.getScheme());
        partGroup = masterRepository.createNewReplicaGroup(partTable, masterRepository.getColumnByName(partTable.getTableId(), "p_partkey"), partRanges);
        masterRepository.createNewReplicaScheme(partGroup, masterRepository.getColumnByName(partTable.getTableId(), "p_partkey"), getColumnIds(partTable), partSource.getDefaultCompressions());
        
        ordersTable = masterRepository.createNewTable(database.getDatabaseId(), "orders", ordersSource.getColumnNames(), ordersSource.getScheme());
        ordersGroup = masterRepository.createNewReplicaGroup(ordersTable, masterRepository.getColumnByName(ordersTable.getTableId(), "o_orderkey"), ordersRanges);
        masterRepository.createNewReplicaScheme(ordersGroup, masterRepository.getColumnByName(ordersTable.getTableId(), "o_orderkey"), getColumnIds(ordersTable), ordersSource.getDefaultCompressions());

        lineitemTablePart = masterRepository.createNewTable(database.getDatabaseId(), "lineitem_p", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupPart = masterRepository.createNewReplicaGroup(lineitemTablePart, masterRepository.getColumnByName(lineitemTablePart.getTableId(), "l_partkey"), partGroup); // link to partGroup
        masterRepository.createNewReplicaScheme(lineitemGroupPart, masterRepository.getColumnByName(lineitemTablePart.getTableId(), "l_partkey"), getColumnIds(lineitemTablePart), lineitemSource.getDefaultCompressions());

        lineitemTableOrders = masterRepository.createNewTable(database.getDatabaseId(), "lineitem_o", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupOrders = masterRepository.createNewReplicaGroup(lineitemTableOrders, masterRepository.getColumnByName(lineitemTableOrders.getTableId(), "l_orderkey"), ordersGroup); // link to ordersGroup
        masterRepository.createNewReplicaScheme(lineitemGroupOrders, masterRepository.getColumnByName(lineitemTableOrders.getTableId(), "l_orderkey"), getColumnIds(lineitemTableOrders), lineitemSource.getDefaultCompressions());

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
        loadTable (customerTable, customerFile);
        loadTable (partTable, partFile);
        loadTable (ordersTable, ordersFile);
        loadTable (lineitemTablePart, lineitemFile);
        loadTable (lineitemTableOrders, lineitemFile);
    }
    private void loadTable (LVTable table, File inputFile) throws Exception {
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
        DataImportSingleNodeTpchBenchmark program = new DataImportSingleNodeTpchBenchmark();
        program.setUp();
        try {
            LOG.info("started:" + lineitemFile.getName() + " and " + partFile.getName() + " and " + ordersFile.getName() + " and " + customerFile.getName());
            long start = System.currentTimeMillis();
            program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended:" + lineitemFile.getName() + " and " + partFile.getName() + " and " + ordersFile.getName() + " and " + customerFile.getName() + ". elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }
}
