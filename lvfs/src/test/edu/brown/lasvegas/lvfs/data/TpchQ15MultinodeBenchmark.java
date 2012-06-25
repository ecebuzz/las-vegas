package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController.Q15ResultList;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanBJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanCJobController;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Run this after {@link DataImportMultiNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ15MultinodeBenchmark {
    private static final Logger LOG = Logger.getLogger(TpchQ15MultinodeBenchmark.class);

    private final char queryPlan;
    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVDatabase database;

    private LVTable lineitemTable, supplierTable;
    public TpchQ15MultinodeBenchmark(char queryPlan) {
    	this.queryPlan = queryPlan;
    }
    private void setUp (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
        database = metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        assert (database != null);

        supplierTable = metaRepo.getTable(database.getDatabaseId(), "supplier");
        // see DataImportSingleNodeTpchBenchmark for why there are multiple lineitem tables
        boolean copartitioned = queryPlan == 'A';
        lineitemTable = metaRepo.getTable(database.getDatabaseId(), copartitioned ? "lineitem_s" : "lineitem_o");
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public Q15ResultList exec (int date) throws Exception {
        BenchmarkTpchQ15JobParameters params = new BenchmarkTpchQ15JobParameters();
        params.setDate(date);
        params.setLineitemTableId(lineitemTable.getTableId());
        params.setSupplierTableId(supplierTable.getTableId());
        BenchmarkTpchQ15JobController controller;
        switch (queryPlan) {
        case 'A': controller = new BenchmarkTpchQ15PlanAJobController(metaRepo, 1000L, 100L, 100L); break;
        case 'B': controller = new BenchmarkTpchQ15PlanBJobController(metaRepo, 1000L, 100L, 100L); break;
        case 'C': controller = new BenchmarkTpchQ15PlanCJobController(metaRepo, 1000L, 100L, 100L); break;
        default: throw new Exception ("wtf??? : " + queryPlan);
        }

        LOG.info("started Q15-" + queryPlan + "...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q15-" + queryPlan + ":" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }

    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 3) {
            System.err.println("usage: java " + TpchQ15MultinodeBenchmark.class.getName() + " <metadata repository address> <date> <query plan(A/B/C)>");
            System.err.println("ex: java " + TpchQ15MultinodeBenchmark.class.getName() + " poseidon:28710 19960101 A");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        int date = Integer.parseInt(args[1]);
        LOG.info("date=" + date);
        char queryPlan = args[2].charAt(0);
        LOG.info("queryPlan=" + queryPlan);
        
        TpchQ15MultinodeBenchmark program = new TpchQ15MultinodeBenchmark(queryPlan);
        program.setUp(metaRepoAddress);
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            Q15ResultList result = program.exec(date);
            long end = System.currentTimeMillis();
            LOG.info("ended(Q15-" + queryPlan + "): elapsed time=" + (end - start) + "ms. result=" + result);
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }

}
