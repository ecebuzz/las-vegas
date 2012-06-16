package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVJob;
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
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureFromBuddyJobController;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureJobParameters;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcase for recovery from the same replica group.
 * @see RecoverFractureFromBuddyJobController
 */
public class RecoverFractureFromBuddyTest {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(RecoverFractureFromBuddyTest.class);

    private static final File inputFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl"); // just for testing
    
    private static final int nodesPerRack = 2;
    
    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;
    private LVReplicaScheme orderkeyScheme, suppkeyScheme;
    private LVFracture fracture;
    private final ValueRange[] ranges = new ValueRange[]{new ValueRange(ColumnType.BIGINT, null, 33L), new ValueRange(ColumnType.BIGINT, 33L, null)};
    
    private final MiniDataSource dataSource = new MiniTPCHLineitem();

    @Before
    public void setUp () throws IOException {
        this.resources = new SingleNodeBenchmarkResources(1, nodesPerRack, 1);
        if (!inputFile.exists()) {
            throw new FileNotFoundException(inputFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        
        database = resources.databases[0];
        final String[] columnNames = dataSource.getColumnNames();
        columns = new HashMap<String, LVColumn>();
        table = resources.metaRepo.createNewTable(database.getDatabaseId(), "lineorder", columnNames, dataSource.getScheme());
        for (LVColumn column : resources.metaRepo.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }
        columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = resources.metaRepo.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        group = resources.metaRepo.createNewReplicaGroup(table, columns.get("l_orderkey"), ranges);
        orderkeyScheme = resources.metaRepo.createNewReplicaScheme(group, columns.get("l_orderkey"), columnIds, dataSource.getDefaultCompressions());
        suppkeyScheme = resources.metaRepo.createNewReplicaScheme(group, columns.get("l_suppkey"), columnIds, dataSource.getDefaultCompressions());

        // first, load the data
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.addNodeFilePath(resources.nodes[0][0].getNodeId(), inputFile.getAbsolutePath());
        ImportFractureJobController controller = new ImportFractureJobController(resources.metaRepo, 400L, 400L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            assertEquals(TaskStatus.DONE, task.getStatus());
        }
        assertEquals(JobStatus.DONE, job.getStatus());
        fracture = controller.getFracture();
    }
    
    @After
    public void tearDown () throws IOException {
        resources.tearDown();
    }
    
    @Test
    public void recoverOrderkeySchemeFromSuppkeyScheme () throws Exception {
        recoverInternal ("recoverOrderkeySchemeFromSuppkeyScheme", orderkeyScheme, suppkeyScheme, columns.get("l_orderkey"));
    }
    @Test
    public void recoverSuppkeySchemeFromOrderkeyScheme () throws Exception {
        recoverInternal ("recoverSuppkeySchemeFromOrderkeyScheme", suppkeyScheme, orderkeyScheme, columns.get("l_orderkey"));
    }
    private void recoverInternal (String testname, LVReplicaScheme damagedScheme, LVReplicaScheme sourceScheme, LVColumn partitioningColumn) throws Exception {
        LOG.info(testname + ":started:" + inputFile.getName());
        long start = System.currentTimeMillis();
        
        // mark the scheme as damaged
        {
            LVReplica replica = resources.metaRepo.getReplicaFromSchemeAndFracture(damagedScheme.getSchemeId(), fracture.getFractureId());
            resources.metaRepo.updateReplicaStatus(replica, ReplicaStatus.NOT_READY);
            LVReplicaPartition[] partitions = resources.metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assertEquals(ranges.length, partitions.length);
            for (LVReplicaPartition partition : partitions) {
                resources.metaRepo.updateReplicaPartitionNoReturn(partition.getPartitionId(), ReplicaPartitionStatus.LOST, null);
            }
        }
        
        RecoverFractureJobParameters params = new RecoverFractureJobParameters();
        params.setDamagedSchemeId(damagedScheme.getSchemeId());
        params.setFractureId(fracture.getFractureId());
        params.setSourceSchemeId(sourceScheme.getSchemeId());
        RecoverFractureFromBuddyJobController controller = new RecoverFractureFromBuddyJobController(resources.metaRepo, 400L, 400L, 100L);
        LOG.info(testname + ":started the recovery job...");
        LVJob job = controller.startSync(params);
        LOG.info(testname + ":finished the recovery job...:" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info(testname + ":Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            assertEquals(TaskStatus.DONE, task.getStatus());
        }

        long end = System.currentTimeMillis();
        LOG.info(testname + ":ended:" + inputFile.getName() + ". elapsed time=" + (end - start) + "ms");
        assertEquals(JobStatus.DONE, job.getStatus());

        // check if it's recovered
        // TODO the following should be shared with RecoverFractureForeignTest
        {
            LVReplica replica = resources.metaRepo.getReplicaFromSchemeAndFracture(damagedScheme.getSchemeId(), fracture.getFractureId());
            assertEquals(ReplicaStatus.OK, replica.getStatus());
            LVReplicaPartition[] partitions = resources.metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assertEquals(ranges.length, partitions.length);
            
            int totalTuples = 0;
            for (LVReplicaPartition partition : partitions) {
                assertEquals(ReplicaPartitionStatus.OK, partition.getStatus());

                LVColumnFile columnFile = resources.metaRepo.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), partitioningColumn.getColumnId());
                ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
                ColumnFileReaderBundle reader = new ColumnFileReaderBundle(bundle);
                TypedReader<?, ?> dataReader = reader.getDataReader();
                int tup = dataReader.getTotalTuples();
                ValueRange range = ranges[partition.getRange()];
                LOG.info(tup + " tuples in range-" + range);
                assertTrue (tup > 0);
                totalTuples += tup;

                for (int i = 0; i < tup; ++i) {
                    // I just want to do:
                    //   assertTrue (range.contains(dataReader.readValue()));
                    // but this type inference doesn't compile on some version of Java.
                    // so, let's do it explicitly.
                    if (partitioningColumn.getType() == ColumnType.INTEGER) {
                        Integer val = (Integer) dataReader.readValue();
                        assertTrue (range.contains(val));
                    } else {
                        assertEquals (ColumnType.BIGINT, partitioningColumn.getType());
                        Long val = (Long) dataReader.readValue();
                        assertTrue (range.contains(val));
                    }
                }
                reader.close();
            }
            LOG.info("total tuples: " + totalTuples);
            assertEquals(dataSource.getCount(), totalTuples);
        }
    }
}
