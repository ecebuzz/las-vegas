package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17PlanAJobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17PlanBJobController;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Run this after {@link DataImportMultiNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ17MultinodeBenchmark {
    private static final Logger LOG = Logger.getLogger(TpchQ17MultinodeBenchmark.class);

    private final boolean fastQueryPlan;
    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVDatabase database;

    private LVTable lineitemTable, partTable;
    public TpchQ17MultinodeBenchmark(boolean fastQueryPlan) {
    	this.fastQueryPlan = fastQueryPlan;
    }
    private void setUp (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
        database = metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        assert (database != null);

        partTable = metaRepo.getTable(database.getDatabaseId(), "part");
        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
        lineitemTable = metaRepo.getTable(database.getDatabaseId(), fastQueryPlan ? "lineitem_p" : "lineitem_o");
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public double exec (String brand, String container) throws Exception {
        BenchmarkTpchQ17JobParameters params = new BenchmarkTpchQ17JobParameters();
        params.setBrand(brand);
        params.setContainer(container);
        params.setLineitemTableId(lineitemTable.getTableId());
        params.setPartTableId(partTable.getTableId());
        BenchmarkTpchQ17JobController controller;
        if (fastQueryPlan) {
        	controller = new BenchmarkTpchQ17PlanAJobController(metaRepo, 1000L, 100L, 100L);
        } else {
        	controller = new BenchmarkTpchQ17PlanBJobController(metaRepo, 1000L, 100L, 100L);
        }

        LOG.info("started Q17(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q17(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "):" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }
    private static String trimQuote (String str) {
        while (str.startsWith("\"") || str.startsWith("'")) {
            str = str.substring(1);
        }
        while (str.endsWith("\"") || str.endsWith("'")) {
            str = str.substring(0, str.length() - 1);
        }
        return str.trim();
    }

    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 4) {
            System.err.println("usage: java " + TpchQ17MultinodeBenchmark.class.getName() + " <metadata repository address> <brand> <container> <whether to use faster query plan>");
            System.err.println("ex: java " + TpchQ17MultinodeBenchmark.class.getName() + " poseidon:28710 \"Brand#34\" \"MED DRUM\" true");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        String brand = trimQuote(args[1]);
        LOG.info("brand=" + brand);
        String container = trimQuote(args[2]);
        LOG.info("container=" + container);
        boolean fastQueryPlan = new Boolean(args[3]);
        LOG.info("fastQueryPlan=" + fastQueryPlan);
        
        TpchQ17MultinodeBenchmark program = new TpchQ17MultinodeBenchmark(fastQueryPlan);
        program.setUp(metaRepoAddress);
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            double result = program.exec(brand, container);
            long end = System.currentTimeMillis();
            LOG.info("ended(" + (fastQueryPlan ? "assume co-partitioning" : "slower query plan") + "). result=" + result + " elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }

}
