package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import javax.print.attribute.standard.JobState;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;
import edu.brown.lasvegas.util.ValueRange;

/**
 * TPCH version.
 * @see DataImportSingleNodeTpchBenchmark
 * @see DataImportMultiNodeBenchmark
 */
public class DataImportMultiNodeTpchBenchmark {
    private static final Logger LOG = Logger.getLogger(DataImportMultiNodeTpchBenchmark.class);
    private static int partitionCount = 0;

    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVDatabase database;

    /**
     * A hack for experiments. see DataImportSingleNodeTpchBenchmark.
     */
    private LVTable lineitemTablePart, partTable, lineitemTableOrders, ordersTable, customerTable;
    private LVReplicaGroup lineitemGroupPart, partGroup, lineitemGroupOrders, ordersGroup, customerGroup;

    private final MiniDataSource lineitemSource = new MiniTPCHLineitem();
    private final MiniDataSource partSource = new MiniTPCHPart();
    private final MiniDataSource ordersSource = new MiniTPCHOrders();
    private final MiniDataSource customerSource = new MiniTPCHCustomer();
    
    private int[] getColumnIds (LVTable table) throws IOException {
        LVColumn[] columns = metaRepo.getAllColumnsExceptEpochColumn(table.getTableId());
        int[] columnIds = new int[columns.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = columns[i].getColumnId();
        }
        return columnIds;
    }
    
    private void setUp (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
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
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public void exec (String lineitemInputFileName, String partInputFileName, String customerInputFileName, String ordersInputFileName) throws Exception {
        LVTable[] tables = new LVTable[]{partTable, customerTable, ordersTable, lineitemTablePart, lineitemTableOrders};
        String[] inputFileNames = new String[]{partInputFileName, customerInputFileName, ordersInputFileName, lineitemInputFileName, lineitemInputFileName};
        for (int i = 0; i < tables.length; ++i) {
            ImportFractureJobParameters params = DataImportMultiNodeBenchmark.parseInputFile(metaRepo, tables[i], inputFileNames[i]);
            ImportFractureJobController controller = new ImportFractureJobController(metaRepo, 1000L, 1000L, 100L);
            LOG.info("started the import job...");
            LVJob job = controller.startSync(params);
            LOG.info("finished the import job...:" + job);
            for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
                LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            }
            if (job.getStatus() != JobStatus.DONE) {
                LOG.error("the import was incomplete! cancelling all subsequent jobs");
            	break;
            }
        }
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 6) {
            System.err.println("usage: java " + DataImportMultiNodeTpchBenchmark.class.getName() + " <partitionCount> <metadata repository address> <name of the file that lists input files for lineitem table> <name of the file that lists input files for part table> <name of the file that lists input files for customer table> <name of the file that lists input files for orders table>");
            System.err.println("ex: java " + DataImportMultiNodeTpchBenchmark.class.getName() + " 2 poseidon:28710 inputs_lineitem.txt inputs_part.txt inputs_customer.txt inputs_orders.txt");
            // It should be a partitioned tbl (see TPCH's dbgen manual. eg: ./dbgen -T L -s 4 -S 1 -C 2; ./dbgen -T P -s 4 -S 1 -C 2 ).
            return;
        }
        partitionCount = Integer.parseInt(args[0]);
        if (partitionCount <= 0) {
            throw new IllegalArgumentException ("invalid partition count :" + args[0]);
        }
        LOG.info("partitionCount=" + partitionCount);
        String metaRepoAddress = args[1];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        String lineitemInputFileName = args[2];
        String partInputFileName = args[3];
        String customerInputFileName = args[4];
        String ordersInputFileName = args[5];
        
        DataImportMultiNodeTpchBenchmark program = new DataImportMultiNodeTpchBenchmark();
        program.setUp(metaRepoAddress);
        try {
            LOG.info("started: " + partitionCount + " partitions)");
            long start = System.currentTimeMillis();
            program.exec(lineitemInputFileName, partInputFileName, customerInputFileName, ordersInputFileName);
            long end = System.currentTimeMillis();
            LOG.info("ended: " + partitionCount + " partitions). elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }
}
