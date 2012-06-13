package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
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
    public static final String DB_NAME = "ssbdb";
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(DataImportSingleNodeBenchmark.class);

    // private static final String lvfsRoot = "/tmp/test";
    // private static final File inputFile = new File ("/tmp/lineorder_s1.tbl");

    private static final File inputFile = new File ("../ssb-dbgen/lineorder_s1.tbl");
    // private static final File inputFile = new File ("../ssb-dbgen/lineorder_s4.tbl");
    // private static final File inputFile = new File ("../ssb-dbgen/lineorder_s15.tbl");
    // private static final File inputFile = new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_lineorder.tbl"); // just for testing

    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;

    private final MiniDataSource dataSource = new MiniSSBLineorder();

    public DataImportSingleNodeBenchmark() throws IOException {
        this.resources = new SingleNodeBenchmarkResources(1, 1, 0); // 0 databases because the benchmark creates one itself
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

        database = resources.metaRepo.createNewDatabase(DB_NAME);
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
        group = resources.metaRepo.createNewReplicaGroup(table, columns.get("lo_orderkey"), ranges);
        resources.metaRepo.createNewReplicaScheme(group, columns.get("lo_orderkey"), columnIds, dataSource.getDefaultCompressions());
        resources.metaRepo.createNewReplicaScheme(group, columns.get("lo_suppkey"), columnIds, dataSource.getDefaultCompressions());
        resources.metaRepo.createNewReplicaScheme(group, columns.get("lo_orderdate"), columnIds, dataSource.getDefaultCompressions());
    }
    
    protected void tearDown () throws IOException {
        resources.tearDown();
    }
    
    public void exec () throws Exception {
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.addNodeFilePath(resources.nodes[0][0].getNodeId(), inputFile.getAbsolutePath());
        ImportFractureJobController controller = new ImportFractureJobController(resources.metaRepo, 1000L, 1000L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        DataImportSingleNodeBenchmark program = new DataImportSingleNodeBenchmark();
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
