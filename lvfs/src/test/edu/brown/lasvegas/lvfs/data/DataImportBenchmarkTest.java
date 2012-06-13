package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.assertEquals;

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
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcase for data import benchmark with many nodes and racks.
 */
public class DataImportBenchmarkTest {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(DataImportBenchmarkTest.class);

    private static final File inputFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_lineorder.tbl"); // just for testing
    
    private static final int rackCount = 2; // 3;
    private static final int nodesPerRack = 2; // 20;

    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;
    
    private final MiniDataSource dataSource = new MiniSSBLineorder();

    @Before
    public void setUp () throws IOException {
        this.resources = new SingleNodeBenchmarkResources(rackCount, nodesPerRack, 1);
        if (!inputFile.exists()) {
            throw new FileNotFoundException(inputFile.getAbsolutePath() + " doesn't exist. Have you generated the data?");
        }
        
        this.database = resources.databases[0];
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
        group = resources.metaRepo.createNewReplicaGroup(table, columns.get("lo_orderkey"), ranges);
        resources.metaRepo.createNewReplicaScheme(group, columns.get("lo_orderkey"), columnIds, dataSource.getDefaultCompressions());
        resources.metaRepo.createNewReplicaScheme(group, columns.get("lo_suppkey"), columnIds, dataSource.getDefaultCompressions());
        resources.metaRepo.createNewReplicaScheme(group, columns.get("lo_orderdate"), columnIds, dataSource.getDefaultCompressions());
    }
    
    @After
    public void tearDown () throws IOException {
        resources.tearDown();
    }
    
    @Test
    public void testAll () throws Exception {
        LOG.info("started:" + inputFile.getName());
        long start = System.currentTimeMillis();

        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        // all nodes use the same file. this is fine because we have no unique-constraint
        for (LVRackNode[] array : resources.nodes) {
            for (LVRackNode node : array) {
                params.addNodeFilePath(node.getNodeId(), inputFile.getAbsolutePath());
            }
        }
        ImportFractureJobController controller = new ImportFractureJobController(resources.metaRepo, 400L, 400L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            assertEquals(TaskStatus.DONE, task.getStatus());
        }

        long end = System.currentTimeMillis();
        LOG.info("ended:" + inputFile.getName() + ". elapsed time=" + (end - start) + "ms");
        assertEquals(JobStatus.DONE, job.getStatus());
    }
}
