package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.util.ValueRange;

/**
 * TPCH version.
 * @see DataImportSingleNodeTpchBenchmark
 * @see DataImportMultiNodeBenchmark
 */
public abstract class DataImportTpchBenchmark {
    private static final Logger LOG = Logger.getLogger(DataImportTpchBenchmark.class);

    protected final LVMetadataProtocol metaRepo;
    protected final int partitionCount;
    protected final int factTableFractures;

    protected DataImportTpchBenchmark (LVMetadataProtocol metaRepo, int partitionCount, int factTableFractures) {
        this.metaRepo = metaRepo;
        this.partitionCount = partitionCount;
        this.factTableFractures = factTableFractures;
    }

    public static final String DB_NAME = "tpchdb"; 
    protected LVDatabase database;

    /**
     * a dirty hack for experiments.
     * import two versions of LINEITEM table for two different partitioning.
     * we can just use replica groups for this purpose, but then we can't utilize the limited
     * number of nodes because replica groups in the same fracture must have non-overlapping dedicated racks.
     * If we have 2x more machines in our lab, we don't need this hack.
     */
    protected LVTable lineitemTablePart, partTable, lineitemTableSupplier, supplierTable, lineitemTableOrders, ordersTable, customerTable;
    protected LVReplicaGroup lineitemGroupPart, partGroup, lineitemGroupSupplier, supplierGroup, lineitemGroupOrders, ordersGroup, customerGroup;

    private final MiniDataSource lineitemSource = new MiniTPCHLineitem();
    private final MiniDataSource partSource = new MiniTPCHPart();
    private final MiniDataSource supplierSource = new MiniTPCHSupplier();
    private final MiniDataSource ordersSource = new MiniTPCHOrders();
    private final MiniDataSource customerSource = new MiniTPCHCustomer();
    
    protected int[] getColumnIds (LVTable table) throws IOException {
        LVColumn[] columns = metaRepo.getAllColumnsExceptEpochColumn(table.getTableId());
        int[] columnIds = new int[columns.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = columns[i].getColumnId();
        }
        return columnIds;
    }
    private ValueRange[] createUniformRangesInteger (int interval) {
        ValueRange[] ranges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
        	ranges[i] = new ValueRange ();
        	ranges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
            	ranges[i].setStartKey(null);
            } else {
            	ranges[i].setStartKey(interval * i + 1);
            }
            if (i == partitionCount - 1) {
            	ranges[i].setEndKey(null);
            } else {
            	ranges[i].setEndKey(interval * (i + 1) + 1);
            }
        }
        return ranges;
    }
    private ValueRange[] createUniformRangesBigint (long interval) {
        ValueRange[] ranges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
        	ranges[i] = new ValueRange ();
        	ranges[i].setType(ColumnType.BIGINT);
            if (i == 0) {
            	ranges[i].setStartKey(null);
            } else {
            	ranges[i].setStartKey(interval * i + 1L);
            }
            if (i == partitionCount - 1) {
            	ranges[i].setEndKey(null);
            } else {
            	ranges[i].setEndKey(interval * (i + 1) + 1L);
            }
        }
        return ranges;
    }
    public void setUp () throws IOException {
        if (metaRepo.getDatabase(DB_NAME) != null) {
            metaRepo.dropDatabase(metaRepo.getDatabase(DB_NAME).getDatabaseId());
            LOG.info("dropped existing database");
        }

        database = metaRepo.createNewDatabase(DB_NAME);

        ValueRange[] customerRanges = createUniformRangesInteger(150000);
        ValueRange[] partRanges = createUniformRangesInteger(200000);
        ValueRange[] supplierRanges = createUniformRangesInteger(10000);
        ValueRange[] ordersRanges = createUniformRangesBigint(6000000L);

        customerTable = metaRepo.createNewTable(database.getDatabaseId(), "customer", customerSource.getColumnNames(), customerSource.getScheme());
        customerGroup = metaRepo.createNewReplicaGroup(customerTable, metaRepo.getColumnByName(customerTable.getTableId(), "c_custkey"), customerRanges);
        metaRepo.createNewReplicaScheme(customerGroup, metaRepo.getColumnByName(customerTable.getTableId(), "c_custkey"), getColumnIds(customerTable), customerSource.getDefaultCompressions());

        partTable = metaRepo.createNewTable(database.getDatabaseId(), "part", partSource.getColumnNames(), partSource.getScheme());
        partGroup = metaRepo.createNewReplicaGroup(partTable, metaRepo.getColumnByName(partTable.getTableId(), "p_partkey"), partRanges);
        metaRepo.createNewReplicaScheme(partGroup, metaRepo.getColumnByName(partTable.getTableId(), "p_partkey"), getColumnIds(partTable), partSource.getDefaultCompressions());

        supplierTable = metaRepo.createNewTable(database.getDatabaseId(), "supplier", supplierSource.getColumnNames(), supplierSource.getScheme());
        supplierGroup = metaRepo.createNewReplicaGroup(supplierTable, metaRepo.getColumnByName(supplierTable.getTableId(), "s_suppkey"), supplierRanges);
        metaRepo.createNewReplicaScheme(supplierGroup, metaRepo.getColumnByName(supplierTable.getTableId(), "s_suppkey"), getColumnIds(supplierTable), supplierSource.getDefaultCompressions());
        
        ordersTable = metaRepo.createNewTable(database.getDatabaseId(), "orders", ordersSource.getColumnNames(), ordersSource.getScheme());
        ordersGroup = metaRepo.createNewReplicaGroup(ordersTable, metaRepo.getColumnByName(ordersTable.getTableId(), "o_orderkey"), ordersRanges);
        metaRepo.createNewReplicaScheme(ordersGroup, metaRepo.getColumnByName(ordersTable.getTableId(), "o_orderkey"), getColumnIds(ordersTable), ordersSource.getDefaultCompressions());

        lineitemTablePart = metaRepo.createNewTable(database.getDatabaseId(), "lineitem_p", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupPart = metaRepo.createNewReplicaGroup(lineitemTablePart, metaRepo.getColumnByName(lineitemTablePart.getTableId(), "l_partkey"), partGroup); // link to partGroup
        metaRepo.createNewReplicaScheme(lineitemGroupPart, metaRepo.getColumnByName(lineitemTablePart.getTableId(), "l_partkey"), getColumnIds(lineitemTablePart), lineitemSource.getDefaultCompressions());

        lineitemTableSupplier = metaRepo.createNewTable(database.getDatabaseId(), "lineitem_s", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupSupplier = metaRepo.createNewReplicaGroup(lineitemTableSupplier, metaRepo.getColumnByName(lineitemTableSupplier.getTableId(), "l_suppkey"), supplierGroup); // link to supplierGroup
        metaRepo.createNewReplicaScheme(lineitemGroupSupplier, metaRepo.getColumnByName(lineitemTableSupplier.getTableId(), "l_suppkey"), getColumnIds(lineitemTableSupplier), lineitemSource.getDefaultCompressions());

        lineitemTableOrders = metaRepo.createNewTable(database.getDatabaseId(), "lineitem_o", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupOrders = metaRepo.createNewReplicaGroup(lineitemTableOrders, metaRepo.getColumnByName(lineitemTableOrders.getTableId(), "l_orderkey"), ordersGroup); // link to ordersGroup
        metaRepo.createNewReplicaScheme(lineitemGroupOrders, metaRepo.getColumnByName(lineitemTableOrders.getTableId(), "l_orderkey"), getColumnIds(lineitemTableOrders), lineitemSource.getDefaultCompressions());
    }
    protected abstract void tearDown () throws IOException;

    public final void exec (String lineitemInputFileName, String partInputFileName, String supplierInputFileName, String customerInputFileName, String ordersInputFileName) throws Exception {
        try {
            long start = System.currentTimeMillis();
 
            LVTable[] tables = new LVTable[]{partTable, supplierTable, customerTable, ordersTable, lineitemTablePart, lineitemTableOrders, lineitemTableSupplier};
            String[] inputFileNames = new String[]{partInputFileName, supplierInputFileName, customerInputFileName, ordersInputFileName, lineitemInputFileName, lineitemInputFileName, lineitemInputFileName};
            for (int i = 0; i < tables.length; ++i) {
                int tableFractures = 1;
                // only fact tables (orders/lineitem) could be fractured
                if (tables[i] == ordersTable || tables[i] == lineitemTablePart || tables[i] == lineitemTableSupplier ||tables[i] == lineitemTableOrders) {
                    tableFractures = factTableFractures;
                }
                
                // to save time, run all fracture import at the same time.
                // if #fractures is large, partitioning the input files and loading them are much faster with parallel execution.
                int[] jobIds = new int[tableFractures]; 
                ImportFractureJobController[] controllers = new ImportFractureJobController[tableFractures]; 
                SortedSet<Integer> remainings = new TreeSet<Integer> ();
                for (int currentFracture = 0; currentFracture < tableFractures; ++currentFracture) {
                    ImportFractureJobParameters params = DataImportMultiNodeBenchmark.parseInputFile(metaRepo, tables[i], inputFileNames[i], tableFractures, currentFracture);
                    ImportFractureJobController controller = new ImportFractureJobController(metaRepo, 1000L, 1000L, 100L);
                    LOG.info("started the import job (" + tables[i].getName() + ": " + currentFracture + "/" + tableFractures + ")...");
                    // LVJob job = controller.startSync(params);
                    LVJob job = controller.startAsync(params); // asynchronous return to all at once
                    controllers[currentFracture] = controller;
                    jobIds[currentFracture] = job.getJobId();
                    remainings.add (currentFracture);
                }
                // then, wait until all of them ends
                boolean hadError = false;
                while (!remainings.isEmpty()) {
                    Thread.sleep(1000L);
                    ArrayList<Integer> doneFractures = new ArrayList<Integer>();
                    for (Integer currentFracture : remainings) {
                        LVJob job = metaRepo.getJob(jobIds[currentFracture]);
                        assert (job != null);
                        if (JobStatus.isFinished(job.getStatus())) {
                            LOG.info("finished (status=" + job.getStatus() + ") the import job (" + tables[i].getName() + ": " + currentFracture + "/" + tableFractures + "):" + job);
                            for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
                                LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
                            }
                            doneFractures.add(currentFracture); // remove later to avoid 'modification in iterator'
                            if (job.getStatus() != JobStatus.DONE) {
                                LOG.error("the import was incomplete! cancelling all subsequent jobs");
                                hadError = true;
                            }
                        }
                    }
                    remainings.removeAll(doneFractures);
                    if (hadError) {
                        // cancel all running data import
                        for (Integer currentFracture : remainings) {
                            controllers[currentFracture].stop();
                        }
                        break;
                    }
                }
                if (hadError) {
                    LOG.error("at least one data import failed. cancelled all data import jobs.");
                }
            }
            long end = System.currentTimeMillis();
            LOG.info("all import done in " + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            tearDown();
        }
        LOG.info("exit");
    }
}
