package edu.brown.lasvegas;

import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.MergeFractureJobController;

/**
 * Defines types of Jobs ({@link LVJob}).
 */
public enum JobType {
    /**
     * A job to import data into LVFS as a new fracture.
     * @see ImportFractureJobController
     */
    IMPORT_FRACTURE,
    /**
     * A job to merge fractures.
     * @see MergeFractureJobController
     */
    MERGE_FRACTURE,
    /** A job to recover all files of a replica scheme from another replica scheme in the same group. */ 
    RECOVER_FRACTURE_FROM_BUDDY,
    
    
    /** A job to process a user-issued query.*/
    QUERY,

    // jobs below are for benchmarks or testing.
    
    /**
     * This job runs TPC-H's Q17, assuming a single fracture,
     * and a co-partitioned part and lineitem table.
     * @see BenchmarkTpchQ17JobController
     */
    BENCHMARK_TPCH_Q17,
    
    /** kind of null. */
    INVALID,
}
