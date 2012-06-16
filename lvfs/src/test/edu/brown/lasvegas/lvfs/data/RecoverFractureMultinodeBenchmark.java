package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Run this after {@link DataImportMultiNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class RecoverFractureMultinodeBenchmark extends RecoverFractureBenchmark {
    private static final Logger LOG = Logger.getLogger(RecoverFractureMultinodeBenchmark.class);
    private final LVMetadataClient client;

    public RecoverFractureMultinodeBenchmark(LVMetadataClient client, boolean foreignRecovery) throws IOException {
        super(client.getChannel(), foreignRecovery);
        this.client = client;
    }
    public void tearDown () throws IOException {
        client.release();
    }

    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node recovery experiment..");
        if (args.length < 2) {
            System.err.println("usage: java " + RecoverFractureMultinodeBenchmark.class.getName() + " <metadata repository address> <delta days> <whether it is foreign recovery with repartitioning>");
            System.err.println("ex: java " + RecoverFractureMultinodeBenchmark.class.getName() + " poseidon:28710 true");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        boolean foreignRecovery = new Boolean(args[1]);
        LOG.info("foreignRecovery=" + foreignRecovery);
        
        Configuration conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metaRepoAddress);
        LVMetadataClient client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metaRepoAddress);

        RecoverFractureMultinodeBenchmark program = new RecoverFractureMultinodeBenchmark(client, foreignRecovery);
        try {
            program.exec();
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }

}
