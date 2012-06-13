package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobParameters;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobController.Q1ResultSet;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Run this after {@link DataImportMultiNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class TpchQ1MultinodeBenchmark {
    private static final Logger LOG = Logger.getLogger(TpchQ1MultinodeBenchmark.class);

    private final boolean partQueryPlan;
    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVDatabase database;
    private LVTable table;
    private LVReplicaScheme scheme;

    public TpchQ1MultinodeBenchmark(boolean partQueryPlan) {
    	this.partQueryPlan = partQueryPlan;
    }
    private void setUp (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
        database = metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        assert (database != null);

        // see DataImportSingleNodeTpchBenchmark for why there are two lineitem tables
        table = metaRepo.getTable(database.getDatabaseId(), partQueryPlan ? "lineitem_p" : "lineitem_o");
        LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(table.getTableId());
        LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(groups[0].getGroupId());
        scheme = schemes[0];
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public Q1ResultSet exec (int deltaDays) throws Exception {
        BenchmarkTpchQ1JobParameters params = new BenchmarkTpchQ1JobParameters();
        params.setDeltaDays(deltaDays);
        params.setSchemeId(scheme.getSchemeId());
        params.setTableId(table.getTableId());
        BenchmarkTpchQ1JobController controller = new BenchmarkTpchQ1JobController(metaRepo, 1000L, 100L, 100L);

        LOG.info("started Q1(" + table.getName() + ")...");
        LVJob job = controller.startSync(params);
        LOG.info("finished Q1(" + table.getName() + "):" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        return controller.getQueryResult();
    }

    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 3) {
            System.err.println("usage: java " + TpchQ1MultinodeBenchmark.class.getName() + " <metadata repository address> <delta days> <whether to use partkey-partitioned lineitem>");
            System.err.println("ex: java " + TpchQ1MultinodeBenchmark.class.getName() + " poseidon:28710 90 true");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        int deltaDays = Integer.parseInt(args[1]);
        LOG.info("deltaDays=" + deltaDays);
        boolean partQueryPlan = new Boolean(args[2]);
        LOG.info("partQueryPlan=" + partQueryPlan);
        
        TpchQ1MultinodeBenchmark program = new TpchQ1MultinodeBenchmark(partQueryPlan);
        program.setUp(metaRepoAddress);
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            Q1ResultSet result = program.exec(deltaDays);
            long end = System.currentTimeMillis();
            LOG.info("ended(partQueryPlan=" + partQueryPlan + "): elapsed time=" + (end - start) + "ms");
            LOG.info("\r\n" + result.toString());
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }

}
