package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.LVFSFileType;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobParameters;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ17SingleNodeBenchmark {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE_ADDRESS = "localhost:12345";
    private static final String DATANODE_NAME = "node";
    private static final Logger LOG = Logger.getLogger(TpchQ17SingleNodeBenchmark.class);

    private MasterMetadataRepository masterRepository;
    private String rootDir;
    private String tmpDir;
    private LVDataNode dataNode;
    private Configuration conf;

    private LVDatabase database;
    private LVTable lineitemTable, partTable;

    private void setUp () throws IOException {
        masterRepository = new MasterMetadataRepository(false, TEST_BDB_HOME); // keep the existing BDB
        database = masterRepository.getDatabase("db1");
        
        partTable = masterRepository.getTable(database.getDatabaseId(), "part");
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
    	lineitemTable = masterRepository.getTable(database.getDatabaseId(), fastQueryPlan ? "lineitem_p" : "lineitem_o");
        // get the data folder by checking one file
        {
            LVFracture fracture = masterRepository.getAllFractures(lineitemTable.getTableId())[0];
            LVReplica replica = masterRepository.getAllReplicasByFractureId(fracture.getFractureId())[0];
            LVReplicaPartition partition = masterRepository.getAllReplicaPartitionsByReplicaId(replica.getReplicaId())[0];
            LVColumnFile file = masterRepository.getAllColumnFilesByReplicaPartitionId(partition.getPartitionId())[0];
            LOG.info("a path looks like: " + file.getLocalFilePath());
            LVFSFilePath path = new LVFSFilePath(LVFSFileType.DATA_FILE.appendExtension(file.getLocalFilePath()));
            rootDir = path.getLvfsRootDir();
            LOG.info("the root dir should be: " + rootDir);
        }

        conf = new Configuration();
        tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
        conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
        conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
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

    private static final String brand = "Brand#34";
    private static final String container = "MED DRUM";
    private static final boolean fastQueryPlan = true;
    public double exec () throws Exception {
        BenchmarkTpchQ17JobParameters params = new BenchmarkTpchQ17JobParameters();
        params.setBrand(brand);
        params.setContainer(container);
        params.setLineitemTableId(lineitemTable.getTableId());
        params.setPartTableId(partTable.getTableId());
        BenchmarkTpchQ17JobController controller = new BenchmarkTpchQ17JobController(masterRepository, 1000L, 100L, 100L);
        LOG.info("started Q17...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q17:" + job);
        for (LVTask task : masterRepository.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        TpchQ17SingleNodeBenchmark program = new TpchQ17SingleNodeBenchmark();
        program.setUp();
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            double result = program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended: result=" + result + ". elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }

}
