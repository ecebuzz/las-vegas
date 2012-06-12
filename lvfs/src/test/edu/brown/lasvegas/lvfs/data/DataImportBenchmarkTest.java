package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ClearAllTest;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcase for data import benchmark with many nodes and racks.
 */
public class DataImportBenchmarkTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final Logger LOG = Logger.getLogger(DataImportBenchmarkTest.class);

    private static final String lvfsRoot = "test";
    private static final File inputFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_lineorder.tbl"); // just for testing
    
    private static final int rackCount = 2; // 3;
    private static final int nodesPerRack = 2; // 20;

    private MasterMetadataRepository masterRepository;
    private final LVDataNode[][] dataNodes = new LVDataNode[rackCount][nodesPerRack];
    private final LVRack[] racks = new LVRack[rackCount];
    private final LVRackNode[][] nodes = new LVRackNode[rackCount][nodesPerRack];
    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;
    
    private final MiniDataSource dataSource = new MiniSSBLineorder();

    @Before
    public void setUp () throws IOException {
        ClearAllTest.deleteFileRecursive(new File("test"));
        if (!inputFile.exists()) {
            throw new FileNotFoundException(inputFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        
        for (int i = 0; i < rackCount; ++i) {
            racks[i] = masterRepository.createNewRack("rack" + i);
            for (int j = 0; j < nodesPerRack; ++j) {
                nodes[i][j] = masterRepository.createNewRackNode(racks[i], "node_" + i + "_" + j, "localhost:" + (12350 + i * nodesPerRack + j));
            }
        }
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
        int partitionCount = rackCount * nodesPerRack;
        ValueRange[] ranges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
            ranges[i] = new ValueRange ();
            ranges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
                ranges[i].setStartKey(null);
            } else {
                ranges[i].setStartKey(i + 1);
            }
            if (i == partitionCount - 1) {
                ranges[i].setEndKey(null);
            } else {
                ranges[i].setEndKey((i + 1) + 1);
            }
        }
        group = masterRepository.createNewReplicaGroup(table, columns.get("lo_orderkey"), ranges);
        masterRepository.createNewReplicaScheme(group, columns.get("lo_orderkey"), columnIds, dataSource.getDefaultCompressions());
        masterRepository.createNewReplicaScheme(group, columns.get("lo_suppkey"), columnIds, dataSource.getDefaultCompressions());
        masterRepository.createNewReplicaScheme(group, columns.get("lo_orderdate"), columnIds, dataSource.getDefaultCompressions());

        
        for (int i = 0; i < rackCount; ++i) {
            for (int j = 0; j < nodesPerRack; ++j) {
                LVRackNode node = nodes[i][j];
                Configuration conf = new Configuration();
                String rootDir = lvfsRoot + "/node_" + i + "_"+ j + "_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
                String tmpDir = rootDir + "/tmp";
                conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
                conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
                conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
                conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
                conf.set(LVDataNode.DATA_ADDRESS_KEY, node.getAddress());
                conf.set(LVDataNode.DATA_NODE_NAME_KEY, node.getName());
                conf.set(LVDataNode.DATA_RACK_NAME_KEY, racks[i].getName());
                dataNodes[i][j] = new LVDataNode(conf, masterRepository);
                dataNodes[i][j].start(null);
            }
        }
    }
    
    @After
    public void tearDown () throws IOException {
        for (int i = 0; i < rackCount; ++i) {
            for (int j = 0; j < nodesPerRack; ++j) {
                dataNodes[i][j].close();
            }
        }
        masterRepository.shutdown();
    }
    
    @Test
    public void testAll () throws Exception {
        LOG.info("started:" + inputFile.getName());
        long start = System.currentTimeMillis();

        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        // all nodes use the same file. it's possible because we have no unique-constraint
        for (int i = 0; i < rackCount; ++i) {
            for (int j = 0; j < nodesPerRack; ++j) {
                LVRackNode node = nodes[i][j];
                params.addNodeFilePath(node.getNodeId(), inputFile.getAbsolutePath());
            }
        }
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 400L, 400L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : masterRepository.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            assertEquals(TaskStatus.DONE, task.getStatus());
        }

        long end = System.currentTimeMillis();
        LOG.info("ended:" + inputFile.getName() + ". elapsed time=" + (end - start) + "ms");
        assertEquals(JobStatus.DONE, job.getStatus());
    }
}
