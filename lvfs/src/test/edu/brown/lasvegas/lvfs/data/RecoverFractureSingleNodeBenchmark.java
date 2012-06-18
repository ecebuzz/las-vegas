package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Run this after {@link DataImportSingleNodeTpchBenchmark}.
 * This is NOT a test case.
 */
public class RecoverFractureSingleNodeBenchmark extends RecoverFractureBenchmark {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(RecoverFractureSingleNodeBenchmark.class);
    
    public RecoverFractureSingleNodeBenchmark(SingleNodeBenchmarkResources resources, boolean foreignRecovery) throws IOException {
        super (resources.metaRepo, foreignRecovery, lostPartitionCount);
        this.resources = resources;
    }
    
    public void tearDown () throws IOException {
        resources.tearDown();
    }

    private static final boolean foreign = true;
    private static final int lostPartitionCount = 2;
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        SingleNodeBenchmarkResources resources = new SingleNodeBenchmarkResources();
        RecoverFractureSingleNodeBenchmark program = new RecoverFractureSingleNodeBenchmark(resources, foreign);
        try {
            program.exec();
        } finally {
            program.tearDown();
            program = null;
        }
        LOG.info("exit");
    }
}
