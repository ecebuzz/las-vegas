package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17PlanBJobController;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ17SingleNodeBenchmark {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(TpchQ17SingleNodeBenchmark.class);

    private LVDatabase database;
    private LVTable lineitemTable, partTable;

    public TpchQ17SingleNodeBenchmark () throws IOException {
        this.resources = new SingleNodeBenchmarkResources();
        this.database = resources.metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        
        partTable = resources.metaRepo.getTable(database.getDatabaseId(), "part");
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
    	lineitemTable = resources.metaRepo.getTable(database.getDatabaseId(), fastQueryPlan ? "lineitem_p" : "lineitem_o");
    }
    
    public void tearDown () throws IOException {
        resources.tearDown();
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
        BenchmarkTpchQ17JobController controller;
        if (fastQueryPlan) {
        	controller = new BenchmarkTpchQ17PlanAJobController(resources.metaRepo, 1000L, 100L, 100L);
        } else {
        	controller = new BenchmarkTpchQ17PlanBJobController(resources.metaRepo, 1000L, 100L, 100L);
        }
        LOG.info("started Q17(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q17(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "):" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        TpchQ17SingleNodeBenchmark program = new TpchQ17SingleNodeBenchmark();
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            double result = program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "): result=" + result + ". elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }

}
