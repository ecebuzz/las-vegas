package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController.Q18ResultRanking;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18PlanBJobController;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ18SingleNodeBenchmark {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(TpchQ18SingleNodeBenchmark.class);

    private LVDatabase database;
    private LVTable lineitemTable, ordersTable, customerTable;

    public TpchQ18SingleNodeBenchmark() throws IOException {
        this.resources = new SingleNodeBenchmarkResources();
        this.database = resources.metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        customerTable = resources.metaRepo.getTable(database.getDatabaseId(), "customer");
        assert (customerTable != null);
        ordersTable = resources.metaRepo.getTable(database.getDatabaseId(), "orders");
        assert (ordersTable != null);
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
        // note, Q18 uses lineitem_o for faster query plan. opposite to Q17!
    	lineitemTable = resources.metaRepo.getTable(database.getDatabaseId(), fastQueryPlan ? "lineitem_o" : "lineitem_p");
        assert (lineitemTable != null);
    }
    
    public void tearDown () throws IOException {
        resources.tearDown();
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
        	controller = new BenchmarkTpchQ18PlanAJobController(resources.metaRepo, 1000L, 100L, 100L);
        } else {
        	controller = new BenchmarkTpchQ18PlanBJobController(resources.metaRepo, 1000L, 100L, 100L);
        }
        LOG.info("started Q18(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q18(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "):" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        TpchQ18SingleNodeBenchmark program = new TpchQ18SingleNodeBenchmark();
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
