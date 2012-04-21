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
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController.Q18ResultRanking;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18PlanBJobController;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ18SingleNodeBenchmark {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE_ADDRESS = "localhost:12345";
    private static final String DATANODE_NAME = "node";
    private static final Logger LOG = Logger.getLogger(TpchQ18SingleNodeBenchmark.class);

    private MasterMetadataRepository masterRepository;
    private String rootDir;
    private String tmpDir;
    private LVDataNode dataNode;
    private Configuration conf;

    private LVDatabase database;
    private LVTable lineitemTable, ordersTable, customerTable;

    private void setUp () throws IOException {
        masterRepository = new MasterMetadataRepository(false, TEST_BDB_HOME); // keep the existing BDB
        database = masterRepository.getDatabase("db1");
        
        customerTable = masterRepository.getTable(database.getDatabaseId(), "customer");
        assert (customerTable != null);
        ordersTable = masterRepository.getTable(database.getDatabaseId(), "orders");
        assert (ordersTable != null);
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
        // note, Q18 uses lineitem_o for faster query plan. opposite to Q17!
    	lineitemTable = masterRepository.getTable(database.getDatabaseId(), fastQueryPlan ? "lineitem_o" : "lineitem_p");
        assert (lineitemTable != null);
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

    private static final double quantityThreshold = 312;
    private static final boolean fastQueryPlan = true;
    public Q18ResultRanking exec () throws Exception {
        BenchmarkTpchQ18JobParameters params = new BenchmarkTpchQ18JobParameters();
        params.setQuantityThreshold(quantityThreshold);
        params.setLineitemTableId(lineitemTable.getTableId());
        params.setOrdersTableId(ordersTable.getTableId());
        params.setCustomerTableId(customerTable.getTableId());
        BenchmarkTpchQ18JobController controller;
        if (fastQueryPlan) {
        	controller = new BenchmarkTpchQ18PlanAJobController(masterRepository, 1000L, 100L, 100L);
        } else {
        	controller = new BenchmarkTpchQ18PlanBJobController(masterRepository, 1000L, 100L, 100L);
        }
        LOG.info("started Q18(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q18(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "):" + job);
        for (LVTask task : masterRepository.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        TpchQ18SingleNodeBenchmark program = new TpchQ18SingleNodeBenchmark();
        program.setUp();
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            Q18ResultRanking result = program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "):  elapsed time=" + (end - start) + "ms. result=" + result);
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }

}
