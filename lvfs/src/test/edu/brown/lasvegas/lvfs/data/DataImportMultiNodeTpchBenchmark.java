package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
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

    private LVTable lineitemTable, partTable;
    private LVReplicaGroup lineitemGroup, partGroup;

    private final MiniDataSource lineitemSource = new MiniTPCHLineitem();
    private final MiniDataSource partSource = new MiniTPCHPart();
    
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

        partTable = metaRepo.createNewTable(database.getDatabaseId(), "part", partSource.getColumnNames(), partSource.getScheme());
        partGroup = metaRepo.createNewReplicaGroup(partTable, metaRepo.getColumnByName(partTable.getTableId(), "p_partkey"), ranges);
        metaRepo.createNewReplicaScheme(partGroup, metaRepo.getColumnByName(partTable.getTableId(), "p_partkey"), getColumnIds(partTable), partSource.getDefaultCompressions());
        
        lineitemTable = metaRepo.createNewTable(database.getDatabaseId(), "lineitem", lineitemSource.getColumnNames(), lineitemSource.getScheme());
        lineitemGroup = metaRepo.createNewReplicaGroup(lineitemTable, metaRepo.getColumnByName(lineitemTable.getTableId(), "l_partkey"), ranges);
        metaRepo.createNewReplicaScheme(lineitemGroup, metaRepo.getColumnByName(lineitemTable.getTableId(), "l_partkey"), getColumnIds(lineitemTable), lineitemSource.getDefaultCompressions());
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public void exec (String lineitemInputFileName, String partInputFileName) throws Exception {
        LVTable[] tables = new LVTable[]{partTable, lineitemTable};
        String[] inputFileNames = new String[]{partInputFileName, lineitemInputFileName};
        for (int i = 0; i < 2; ++i) {
            ImportFractureJobParameters params = DataImportMultiNodeBenchmark.parseInputFile(metaRepo, tables[i], inputFileNames[i]);
            ImportFractureJobController controller = new ImportFractureJobController(metaRepo, 1000L, 1000L, 100L);
            LOG.info("started the import job...");
            LVJob job = controller.startSync(params);
            LOG.info("finished the import job...:" + job);
            for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
                LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
            }
        }
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 4) {
            System.err.println("usage: java " + DataImportMultiNodeTpchBenchmark.class.getName() + " <partitionCount> <metadata repository address> <name of the file that lists input files for lineitem table> <name of the file that lists input files for part table>");
            System.err.println("ex: java " + DataImportMultiNodeTpchBenchmark.class.getName() + " 2 poseidon:28710 inputs_lineitem.txt inputs_part.txt");
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
        
        DataImportMultiNodeTpchBenchmark program = new DataImportMultiNodeTpchBenchmark();
        program.setUp(metaRepoAddress);
        try {
            LOG.info("started: " + partitionCount + " partitions)");
            long start = System.currentTimeMillis();
            program.exec(lineitemInputFileName, partInputFileName);
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
