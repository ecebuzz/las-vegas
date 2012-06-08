package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;
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
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureForeignJobController;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureForeignJobParameters;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcase for recovery from different replica group.
 * @see RecoverFractureForeignJobController
 */
public class ForeignRecoveryTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final Logger LOG = Logger.getLogger(ForeignRecoveryTest.class);

    private static final String lvfsRoot = "test";
    private static final File inputFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl"); // just for testing
    
    private static final int rackCount = 2; // 3;

    private MasterMetadataRepository masterRepository;
    private final LVDataNode[] dataNodes = new LVDataNode[rackCount];
    private final LVRack[] racks = new LVRack[rackCount];
    private final LVRackNode[] nodes = new LVRackNode[rackCount]; // just one node per rack
    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup orderkeyGroup, partkeyGroup;
    private LVReplicaScheme orderkeyScheme, partkeyScheme;
    private LVFracture fracture;
    private final ValueRange[] orderkeyRanges = new ValueRange[]{new ValueRange(ColumnType.BIGINT, null, 33L), new ValueRange(ColumnType.BIGINT, 33L, null)};
    private final ValueRange[] partkeyRanges = new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, 50000), new ValueRange(ColumnType.INTEGER, 50000, 100000), new ValueRange(ColumnType.INTEGER, 100000, null)};
    
    private final MiniDataSource dataSource = new MiniTPCHLineitem();

    @Before
    public void setUp () throws IOException {
        ClearAllTest.deleteFileRecursive(new File("test"));
        if (!inputFile.exists()) {
            throw new FileNotFoundException(inputFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        
        for (int i = 0; i < rackCount; ++i) {
            racks[i] = masterRepository.createNewRack("rack" + i);
            nodes[i] = masterRepository.createNewRackNode(racks[i], "node_" + i, "localhost:" + (12350 + i));
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
        orderkeyGroup = masterRepository.createNewReplicaGroup(table, columns.get("l_orderkey"), orderkeyRanges);
        orderkeyScheme = masterRepository.createNewReplicaScheme(orderkeyGroup, columns.get("l_orderkey"), columnIds, dataSource.getDefaultCompressions());
        partkeyGroup = masterRepository.createNewReplicaGroup(table, columns.get("l_partkey"), partkeyRanges);
        partkeyScheme = masterRepository.createNewReplicaScheme(partkeyGroup, columns.get("l_partkey"), columnIds, dataSource.getDefaultCompressions());

        
        for (int i = 0; i < rackCount; ++i) {
            LVRackNode node = nodes[i];
            Configuration conf = new Configuration();
            String rootDir = lvfsRoot + "/node_" + i + "_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
            String tmpDir = rootDir + "/tmp";
            conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
            conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
            conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
            conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
            conf.set(LVDataNode.DATA_ADDRESS_KEY, node.getAddress());
            conf.set(LVDataNode.DATA_NODE_NAME_KEY, node.getName());
            conf.set(LVDataNode.DATA_RACK_NAME_KEY, racks[i].getName());
            dataNodes[i] = new LVDataNode(conf, masterRepository);
            dataNodes[i].start(null);
        }

        // first, load the data
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(nodes[0].getNodeId(), new String[]{inputFile.getAbsolutePath()});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 400L, 400L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : masterRepository.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            assertEquals(TaskStatus.DONE, task.getStatus());
        }
        assertEquals(JobStatus.DONE, job.getStatus());
        fracture = controller.getFracture();
    }
    
    @After
    public void tearDown () throws IOException {
        for (int i = 0; i < rackCount; ++i) {
            dataNodes[i].close();
        }
        masterRepository.shutdown();
    }
    
    @Test
    public void recoverOrderkeyGroupFromPartkeyGroup () throws Exception {
        recoverInternal ("recoverOrderkeyGroupFromPartkeyGroup", orderkeyScheme, partkeyScheme, columns.get("l_orderkey"), orderkeyRanges);
    }
    @Test
    public void recoverPartkeyGroupFromOrderkeyGroup () throws Exception {
        recoverInternal ("recoverPartkeyGroupFromOrderkeyGroup", partkeyScheme, orderkeyScheme, columns.get("l_partkey"), partkeyRanges);
    }
    private void recoverInternal (String testname, LVReplicaScheme damagedScheme, LVReplicaScheme sourceScheme, LVColumn partitioningColumn, ValueRange[] ranges) throws Exception {
        LOG.info(testname + ":started:" + inputFile.getName());
        long start = System.currentTimeMillis();
        
        // mark the group as damaged
        {
            LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(damagedScheme.getSchemeId(), fracture.getFractureId());
            masterRepository.updateReplicaStatus(replica, ReplicaStatus.NOT_READY);
            LVReplicaPartition[] partitions = masterRepository.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assertEquals(ranges.length, partitions.length);
            for (LVReplicaPartition partition : partitions) {
                masterRepository.updateReplicaPartitionNoReturn(partition.getPartitionId(), ReplicaPartitionStatus.LOST, null);
            }
        }
        
        RecoverFractureForeignJobParameters params = new RecoverFractureForeignJobParameters();
        params.setDamagedSchemeId(damagedScheme.getSchemeId());
        params.setFractureId(fracture.getFractureId());
        params.setSourceSchemeId(sourceScheme.getSchemeId());
        RecoverFractureForeignJobController controller = new RecoverFractureForeignJobController(masterRepository, 400L, 400L, 100L);
        LOG.info(testname + ":started the recovery job...");
        LVJob job = controller.startSync(params);
        LOG.info(testname + ":finished the recovery job...:" + job);
        for (LVTask task : masterRepository.getAllTasksByJob(job.getJobId())) {
            LOG.info(testname + ":Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            assertEquals(TaskStatus.DONE, task.getStatus());
        }

        long end = System.currentTimeMillis();
        LOG.info(testname + ":ended:" + inputFile.getName() + ". elapsed time=" + (end - start) + "ms");
        assertEquals(JobStatus.DONE, job.getStatus());

        // check if it's recovered
        {
            LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(damagedScheme.getSchemeId(), fracture.getFractureId());
            assertEquals(ReplicaStatus.OK, replica.getStatus());
            LVReplicaPartition[] partitions = masterRepository.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assertEquals(ranges.length, partitions.length);
            
            int totalTuples = 0;
            for (LVReplicaPartition partition : partitions) {
                assertEquals(ReplicaPartitionStatus.OK, partition.getStatus());

                LVColumnFile columnFile = masterRepository.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), partitioningColumn.getColumnId());
                ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
                ColumnFileReaderBundle reader = new ColumnFileReaderBundle(bundle);
                TypedReader<?, ?> dataReader = reader.getDataReader();
                int tup = dataReader.getTotalTuples();
                ValueRange range = ranges[partition.getRange()];
                LOG.info(tup + " tuples in range-" + range);
                assertTrue (tup > 0);
                totalTuples += tup;

                for (int i = 0; i < tup; ++i) {
                    assertTrue (range.contains(dataReader.readValue()));
                }
                reader.close();
            }
            LOG.info("total tuples: " + totalTuples);
            assertEquals(dataSource.getCount(), totalTuples);
        }
    }
}
