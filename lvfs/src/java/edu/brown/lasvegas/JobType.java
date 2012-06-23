package edu.brown.lasvegas;

import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ17JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobController;
import edu.brown.lasvegas.lvfs.data.job.DiskCacheFlushJobController;
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
    /** A job to recover all files of a replica scheme from another replica scheme in a different group, which requires re-partitioning. */ 
    RECOVER_FRACTURE_FOREIGN,
    
    
    /** A job to process a user-issued query.*/
    QUERY,

    // jobs below are for benchmarks or testing.
    
    /**
     * This job runs TPC-H's Q1, no assumption.
     * The query is so simple.
     * So this implementation works for arbitrary partitioning, sorting, and number of fractures. 
     * @see BenchmarkTpchQ1JobController
     */
    BENCHMARK_TPCH_Q1,

    /**
     * This job runs TPC-H's Q15.
     * Supplier table must have only one fracture while lineitem table can have
     * an arbitrary number of fractures.
     * This query has two query plans, one using a co-partitioned supplier and lineitem table,
     * another using non-copartitioned files.
     * @see BenchmarkTpchQ15JobController
     */
    BENCHMARK_TPCH_Q15,
    /**
     * This job runs TPC-H's Q17.
     * Part table must have only one fracture while lineitem table can have
     * an arbitrary number of fractures.
     * This query has two query plans, one using a co-partitioned part and lineitem table,
     * another using non-copartitioned files.
     * @see BenchmarkTpchQ17JobController
     */
    BENCHMARK_TPCH_Q17,
    /**
     * This job runs TPC-H's Q18.
     * Customer table must have only one fracture. lineitem and orders table can have
     * an arbitrary number of fractures, but the two tables must have the same number of
     * fractures with the same ranges of orderkey. This is usually true as
     * TPC-H data loading is naturally ordered by orderkey.
     * This query also has two plans, plan A (fast one using a co-partitioned orders and lineitem table)
     * and plan B (slow one not using that).
     * @see BenchmarkTpchQ18JobController
     */
    BENCHMARK_TPCH_Q18,
    
    /**
     * Flush the OS's disk cache at each data node. Used while benchmarks.
     * This might internally use /proc/sys/vm/drop_caches (which requires root permission) or just read large files.
     * @see DiskCacheFlushJobController
     */
    DISK_CACHE_FLUSH,
    
    /** kind of null. */
    INVALID,
}
