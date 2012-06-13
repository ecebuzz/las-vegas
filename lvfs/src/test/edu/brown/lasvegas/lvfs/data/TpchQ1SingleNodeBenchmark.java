package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobController.Q1ResultSet;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobParameters;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ1SingleNodeBenchmark {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(TpchQ1SingleNodeBenchmark.class);

    private LVDatabase database;
    private LVTable table;
    private LVReplicaScheme scheme;
    
    public TpchQ1SingleNodeBenchmark() throws IOException {
        this.resources = new SingleNodeBenchmarkResources();
        this.database = resources.metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
    	this.table = resources.metaRepo.getTable(database.getDatabaseId(), partQueryPlan ? "lineitem_p" : "lineitem_o");
        LVReplicaGroup[] groups = resources.metaRepo.getAllReplicaGroups(table.getTableId());
        LVReplicaScheme[] schemes = resources.metaRepo.getAllReplicaSchemes(groups[0].getGroupId());
        this.scheme = schemes[0];
    }
    
    public void tearDown () throws IOException {
        resources.tearDown();
    }

    private static final int deltaDays = 90;
    /** both query plans perform the same, but let's confirm that. */
    private static final boolean partQueryPlan = true;
    public Q1ResultSet exec () throws Exception {
        BenchmarkTpchQ1JobParameters params = new BenchmarkTpchQ1JobParameters();
        params.setDeltaDays(deltaDays);
        params.setSchemeId(scheme.getSchemeId());
        params.setTableId(table.getTableId());
        BenchmarkTpchQ1JobController controller = new BenchmarkTpchQ1JobController(resources.metaRepo, 1000L, 100L, 100L);
        LOG.info("started Q1(" + table.getName() + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q1(" + table.getName() + "):" + job);
        for (LVTask task : resources.metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        TpchQ1SingleNodeBenchmark program = new TpchQ1SingleNodeBenchmark();
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            Q1ResultSet result = program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended(partQueryPlan=" + partQueryPlan + "): elapsed time=" + (end - start) + "ms");
            LOG.info("\r\n" + result.toString());
            
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }

}
