package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

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

    protected LVDatabase database;

    /**
     * a dirty hack for experiments.
     * import two versions of LINEITEM table for two different partitioning.
     * we can just use replica groups for this purpose, but then we can't utilize the limited
     * number of nodes because replica groups in the same fracture must have non-overlapping dedicated racks.
     * If we have 2x more machines in our lab, we don't need this hack.
     */
    protected LVTable lineitemTablePart, partTable, lineitemTableOrders, ordersTable, customerTable;
    protected LVReplicaGroup lineitemGroupPart, partGroup, lineitemGroupOrders, ordersGroup, customerGroup;

    private final MiniDataSource lineitemSource = new MiniTPCHLineitem();
    private final MiniDataSource partSource = new MiniTPCHPart();
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
    
    public void setUp () throws IOException {
        final String dbname = "db1";
        if (metaRepo.getDatabase(dbname) != null) {
            metaRepo.dropDatabase(metaRepo.getDatabase(dbname).getDatabaseId());
            LOG.info("dropped existing database");
        }

        database = metaRepo.createNewDatabase(dbname);

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

        customerTable = metaRepo.createNewTable(database.getDatabaseId(), "customer", customerSource.getColumnNames(), customerSource.getScheme());
        customerGroup = metaRepo.createNewReplicaGroup(customerTable, metaRepo.getColumnByName(customerTable.getTableId(), "c_custkey"), customerRanges);
        metaRepo.createNewReplicaScheme(customerGroup, metaRepo.getColumnByName(customerTable.getTableId(), "c_custkey"), getColumnIds(customerTable), customerSource.getDefaultCompressions());

        partTable = metaRepo.createNewTable(database.getDatabaseId(), "part", partSource.getColumnNames(), partSource.getScheme());
        partGroup = metaRepo.createNewReplicaGroup(partTable, metaRepo.getColumnByName(partTable.getTableId(), "p_partkey"), partRanges);
        metaRepo.createNewReplicaScheme(partGroup, metaRepo.getColumnByName(partTable.getTableId(), "p_partkey"), getColumnIds(partTable), partSource.getDefaultCompressions());
        
        ordersTable = metaRepo.createNewTable(database.getDatabaseId(), "orders", ordersSource.getColumnNames(), ordersSource.getScheme());
        ordersGroup = metaRepo.createNewReplicaGroup(ordersTable, metaRepo.getColumnByName(ordersTable.getTableId(), "o_orderkey"), ordersRanges);
        metaRepo.createNewReplicaScheme(ordersGroup, metaRepo.getColumnByName(ordersTable.getTableId(), "o_orderkey"), getColumnIds(ordersTable), ordersSource.getDefaultCompressions());

        lineitemTablePart = metaRepo.createNewTable(database.getDatabaseId(), "lineitem_p", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupPart = metaRepo.createNewReplicaGroup(lineitemTablePart, metaRepo.getColumnByName(lineitemTablePart.getTableId(), "l_partkey"), partGroup); // link to partGroup
        metaRepo.createNewReplicaScheme(lineitemGroupPart, metaRepo.getColumnByName(lineitemTablePart.getTableId(), "l_partkey"), getColumnIds(lineitemTablePart), lineitemSource.getDefaultCompressions());

        lineitemTableOrders = metaRepo.createNewTable(database.getDatabaseId(), "lineitem_o", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroupOrders = metaRepo.createNewReplicaGroup(lineitemTableOrders, metaRepo.getColumnByName(lineitemTableOrders.getTableId(), "l_orderkey"), ordersGroup); // link to ordersGroup
        metaRepo.createNewReplicaScheme(lineitemGroupOrders, metaRepo.getColumnByName(lineitemTableOrders.getTableId(), "l_orderkey"), getColumnIds(lineitemTableOrders), lineitemSource.getDefaultCompressions());
    }
    protected abstract void tearDown () throws IOException;

    public final void exec (String lineitemInputFileName, String partInputFileName, String customerInputFileName, String ordersInputFileName) throws Exception {
        try {
            long start = System.currentTimeMillis();
 
            LVTable[] tables = new LVTable[]{partTable, customerTable, ordersTable, lineitemTablePart, lineitemTableOrders};
            String[] inputFileNames = new String[]{partInputFileName, customerInputFileName, ordersInputFileName, lineitemInputFileName, lineitemInputFileName};
            for (int i = 0; i < tables.length; ++i) {
                int tableFractures = 1;
                // only fact tables (orders/lineitem) could be fractured
                if (tables[i] == ordersTable || tables[i] == lineitemTablePart ||tables[i] == lineitemTableOrders) {
                    tableFractures = factTableFractures;
                }
                for (int currentFracture = 0; currentFracture < tableFractures; ++currentFracture) {
                    ImportFractureJobParameters params = DataImportMultiNodeBenchmark.parseInputFile(metaRepo, tables[i], inputFileNames[i], tableFractures, currentFracture);
                    ImportFractureJobController controller = new ImportFractureJobController(metaRepo, 1000L, 1000L, 100L);
                    LOG.info("started the import job (" + tables[i].getName() + ": " + currentFracture + "/" + tableFractures + ")...");
                    LVJob job = controller.startSync(params);
                    LOG.info("finished the import job (" + tables[i].getName() + ": " + currentFracture + "/" + tableFractures + "):" + job);
                    for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
                        LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
                    }
                    if (job.getStatus() != JobStatus.DONE) {
                        LOG.error("the import was incomplete! cancelling all subsequent jobs");
                    	break;
                    }
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
