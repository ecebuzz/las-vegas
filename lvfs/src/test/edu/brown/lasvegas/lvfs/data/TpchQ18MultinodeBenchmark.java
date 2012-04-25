package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController.Q18ResultRanking;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18PlanBJobController;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Run this after {@link DataImportMultiNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ18MultinodeBenchmark {
    private static final Logger LOG = Logger.getLogger(TpchQ18MultinodeBenchmark.class);

    private final boolean fastQueryPlan;
    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVDatabase database;

    private LVTable lineitemTable, ordersTable, customerTable;
    public TpchQ18MultinodeBenchmark(boolean fastQueryPlan) {
    	this.fastQueryPlan = fastQueryPlan;
    }
    private void setUp (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
        final String dbname = "db1";
        database = metaRepo.getDatabase(dbname);
        assert (database != null);

        customerTable = metaRepo.getTable(database.getDatabaseId(), "customer");
        assert (customerTable != null);
        ordersTable = metaRepo.getTable(database.getDatabaseId(), "orders");
        assert (ordersTable != null);
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
        // note, Q18 uses lineitem_o for faster query plan. opposite to Q17!
    	lineitemTable = metaRepo.getTable(database.getDatabaseId(), fastQueryPlan ? "lineitem_o" : "lineitem_p");
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public Q18ResultRanking exec (double quantityThreshold) throws Exception {
        BenchmarkTpchQ18JobParameters params = new BenchmarkTpchQ18JobParameters();
        params.setQuantityThreshold(quantityThreshold);
        params.setLineitemTableId(lineitemTable.getTableId());
        params.setOrdersTableId(ordersTable.getTableId());
        params.setCustomerTableId(customerTable.getTableId());
        BenchmarkTpchQ18JobController controller;
        if (fastQueryPlan) {
        	controller = new BenchmarkTpchQ18PlanAJobController(metaRepo, 1000L, 100L, 100L);
        } else {
        	controller = new BenchmarkTpchQ18PlanBJobController(metaRepo, 1000L, 100L, 100L);
        }

        LOG.info("started Q18(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q18(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "):" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }

    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 3) {
            System.err.println("usage: java " + TpchQ18MultinodeBenchmark.class.getName() + " <metadata repository address> <quantity threshold(312-315)> <whether to use faster query plan>");
            System.err.println("ex: java " + TpchQ18MultinodeBenchmark.class.getName() + " poseidon:28710 312 true");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        double quantityThreshold = Double.parseDouble(args[1]);
        LOG.info("quantityThreshold=" + quantityThreshold);
        boolean fastQueryPlan = new Boolean(args[2]);
        LOG.info("fastQueryPlan=" + fastQueryPlan);
        
        TpchQ18MultinodeBenchmark program = new TpchQ18MultinodeBenchmark(fastQueryPlan);
        program.setUp(metaRepoAddress);
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            Q18ResultRanking result = program.exec(quantityThreshold);
            long end = System.currentTimeMillis();
            LOG.info("ended(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "). elapsed time=" + (end - start) + "ms. result=" + result);
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }

}
