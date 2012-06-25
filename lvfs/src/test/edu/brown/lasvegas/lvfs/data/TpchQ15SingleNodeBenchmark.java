package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController.Q15ResultList;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanBJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanCJobController;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ15SingleNodeBenchmark {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(TpchQ15SingleNodeBenchmark.class);

    private LVDatabase database;
    private LVTable lineitemTable, supplierTable;

    @SuppressWarnings ("all") // to shut up "comparing identical expressions"
    public TpchQ15SingleNodeBenchmark () throws IOException {
        this.resources = new SingleNodeBenchmarkResources();
        this.database = resources.metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        
        this.supplierTable = resources.metaRepo.getTable(database.getDatabaseId(), "supplier");
        // see DataImportSingleNodeTpchBenchmark for why there are multiple lineitem tables
        boolean copartitioned = queryPlan == 'A';
        this.lineitemTable = resources.metaRepo.getTable(database.getDatabaseId(), copartitioned ? "lineitem_s" : "lineitem_o");
    }
    
    public void tearDown () throws IOException {
        resources.tearDown();
    }

    private static final int date = 19960101;
    private static final char queryPlan = 'A'; // A,B,C
    public Q15ResultList exec () throws Exception {
        BenchmarkTpchQ15JobParameters params = new BenchmarkTpchQ15JobParameters();
        params.setDate(date);
        params.setLineitemTableId(lineitemTable.getTableId());
        params.setSupplierTableId(supplierTable.getTableId());
        BenchmarkTpchQ15JobController controller;
        switch (queryPlan) {
        case 'A': controller = new BenchmarkTpchQ15PlanAJobController(resources.metaRepo, 1000L, 100L, 100L); break;
        case 'B': controller = new BenchmarkTpchQ15PlanBJobController(resources.metaRepo, 1000L, 100L, 100L); break;
        case 'C': controller = new BenchmarkTpchQ15PlanCJobController(resources.metaRepo, 1000L, 100L, 100L); break;
        default: throw new Exception ("wtf??? : " + queryPlan);
        }
        LOG.info("started Q15-" + queryPlan + "...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q15-" + queryPlan + ":" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        TpchQ15SingleNodeBenchmark program = new TpchQ15SingleNodeBenchmark();
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            Q15ResultList result = program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended(Q15-" + queryPlan + "): elapsed time=" + (end - start) + "ms. result=" + result);
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }

}
