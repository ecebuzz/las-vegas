package edu.brown.lasvegas;

import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15PlanATaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15PlanBTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15TaskParameters;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ17PlanBTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ17TaskParameters;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ17PlanATaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ18PlanATaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ18PlanBTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ18TaskParameters;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ1TaskParameters;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ1TaskRunner;
import edu.brown.lasvegas.lvfs.data.task.DeletePartitionFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.DeletePartitionFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.DeleteTmpFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.DeleteTmpFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.DiskCacheFlushTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.DiskCacheFlushTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.LoadPartitionedTextFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.LoadPartitionedTextFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.MergePartitionSameSchemeTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.MergePartitionSameSchemeTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.PartitionRawTextFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.PartitionRawTextFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.RecoverPartitionFromBuddyTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.RecoverPartitionFromBuddyTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.RecoverPartitionFromRepartitionedFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.RecoverPartitionFromRepartitionedFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskRunner;

/**
 * Defines types of local Tasks ({@link TaskJob}).
 */
public enum TaskType {
    /**
     * Sub task of {@link JobType#IMPORT_FRACTURE}.
     * Given one or more text files in the local filesystem (not HDFS),
     * this task partitions them into local temporary files.
     * This one is easier than PARTITION_HDFS_TEXT_FILES because
     * of the record-boundary issue in HDFS text files. 
     * @see PartitionRawTextFilesTaskRunner
     */
    PARTITION_RAW_TEXT_FILES,
    
    /**
     * Sub task of {@link JobType#IMPORT_FRACTURE}.
     * Given one or more text files in the local HDFS data node,
     * this task partitions them into local temporary files.
     * So far not implemented because of the record-boundary issue.
     * Instead, one can first generate raw files as a MapReduce task
     * and then execute PARTITION_RAW_TEXT_FILES.
     */
    PARTITION_HDFS_TEXT_FILES,

    /**
     * Sub task of {@link JobType#IMPORT_FRACTURE}.
     * Given partitioned text files output by {@link #PARTITION_TEXT_FILES} task,
     * this task collects those text files from local and remote nodes and
     * construct LVFS files in the local drive.
     * This task is for the first replica scheme in each replica group.
     * For other replica schemes in the group (buddy), use
     * #RECOVER_PARTITION_FROM_BUDDY for much better performance.
     * @see LoadPartitionedTextFilesTaskRunner
     */
    LOAD_PARTITIONED_TEXT_FILES,
    
    /**
     * Sub task of {@link JobType#IMPORT_FRACTURE} or recovery jobs such as {@link JobType#RECOVER_FRACTURE_FROM_BUDDY}.
     * Assuming a buddy (another replica scheme in the same replica group) has all
     * column files of a partition, this task reads, sorts, and compresses them to
     * its own column files. This task is supposed to be efficient because the communication will
     * be between nodes in the same rack.
     * @see RecoverPartitionFromBuddyTaskRunner
     */
    RECOVER_PARTITION_FROM_BUDDY,
    
    /**
     * Sub task of {@link JobType#RECOVER_FRACTURE_FOREIGN}.
     * In order to recover a replica from another replica that is in a different replica group,
     * a 'foreign' recovery must repartition the replica. This sub task receives the repartitioned files
     * and reconstructs the damaged partitions from them.
     * @see RecoverPartitionFromRepartitionedFilesTaskRunner
     */
    RECOVER_PARTITION_FROM_REPARTITIONED_FILES,
    
    /**
     * Sub task of {@link JobType#MERGE_FRACTURE}.
     * Given existing ReplicaPartition in the same replica scheme,
     * merge them into one file.
     * This task has low CPU-overhead because it assumes base partitions in the same scheme (sorting).
     * However, it might cause additional network I/O because some other replica scheme might have
     * corresponding partitions in the same node or at least in the same rack although
     * it needs re-sorting to use.
     * Another version of this task (MergePartitionDifferentScheme?) might be added later to see
     * the tradeoff.
     * @see MergePartitionSameSchemeTaskRunner
     */
    MERGE_PARTITION_SAME_SCHEME,
    
    /**
     * Sub task of a few jobs.
     * Physically delete files in specified partitions in the node.
     */
    DELETE_PARTITION_FILES,
    
    /**
     * Sub task of a few jobs.
     * Physically delete temporary files/folders in the node.
     */
    DELETE_TMP_FILES,
    
    /**
     * Sub task of a few jobs, including recovery and query processing.
     * Output partitioned columnar files for the given partitioning column and range (which
     * is probably different from the current partitioning column/range).
     */
    REPARTITION,

    /**
     * Sub task of {@link JobType#QUERY} (maybe other use?).
     * Process projection (evaluate expression etc). If it's purely projection (output column itself),
     * this task is not needed (directly read column files without this).
     */
    PROJECT,

    /**
     * Sub task of {@link JobType#QUERY} (maybe other use?).
     * Apply filtering to column-files of a sub-partition and output the column-files after filtering.
     */
    FILTER_COLUMN_FILES,
    
    // tasks below are for benchmarks and tests
    /**
     * @see JobType#BENCHMARK_TPCH_Q1
     */
    BENCHMARK_TPCH_Q1,
    /**
     * @see JobType#BENCHMARK_TPCH_Q15
     */
    BENCHMARK_TPCH_Q15_PLANA,
    /**
     * @see JobType#BENCHMARK_TPCH_Q15
     */
    BENCHMARK_TPCH_Q15_PLANB,
    /**
     * @see JobType#BENCHMARK_TPCH_Q17
     */
    BENCHMARK_TPCH_Q17_PLANA,
    /**
     * @see JobType#BENCHMARK_TPCH_Q17
     */
    BENCHMARK_TPCH_Q17_PLANB,
    /**
     * @see JobType#BENCHMARK_TPCH_Q18
     */
    BENCHMARK_TPCH_Q18_PLANA,
    /**
     * @see JobType#BENCHMARK_TPCH_Q18
     */
    BENCHMARK_TPCH_Q18_PLANB,

    /**
     * Flush the OS's disk cache at each data node. Used while benchmarks.
     * This might internally use /proc/sys/vm/drop_caches (which requires root permission) or just read large files.
     */
    DISK_CACHE_FLUSH,

    /** kind of null. */
    INVALID,
    ;

    /**
     * Creates a new instance of task-type specific parameter type.
     * Actually, this should be an abstract method of this enum. However,
     * BDB-JE doesn't support storing enum with constant-specific methods,
     * so this is still a static method.
     */
    public static TaskParameters instantiateParameters (TaskType type) {
        switch (type) {
        case PARTITION_RAW_TEXT_FILES:
            return new PartitionRawTextFilesTaskParameters();
        case LOAD_PARTITIONED_TEXT_FILES:
            return new LoadPartitionedTextFilesTaskParameters();
        case RECOVER_PARTITION_FROM_BUDDY:
            return new RecoverPartitionFromBuddyTaskParameters();
        case RECOVER_PARTITION_FROM_REPARTITIONED_FILES:
            return new RecoverPartitionFromRepartitionedFilesTaskParameters();
        case MERGE_PARTITION_SAME_SCHEME:
            return new MergePartitionSameSchemeTaskParameters();
        case DELETE_PARTITION_FILES:
            return new DeletePartitionFilesTaskParameters();
        case DELETE_TMP_FILES:
            return new DeleteTmpFilesTaskParameters();
        case REPARTITION:
        	return new RepartitionTaskParameters();
        case BENCHMARK_TPCH_Q1:
            return new BenchmarkTpchQ1TaskParameters();
        case BENCHMARK_TPCH_Q15_PLANA:
        case BENCHMARK_TPCH_Q15_PLANB:
            return new BenchmarkTpchQ15TaskParameters();
        case BENCHMARK_TPCH_Q17_PLANA:
        case BENCHMARK_TPCH_Q17_PLANB:
            return new BenchmarkTpchQ17TaskParameters();
        case BENCHMARK_TPCH_Q18_PLANA:
        case BENCHMARK_TPCH_Q18_PLANB:
            return new BenchmarkTpchQ18TaskParameters();
        case DISK_CACHE_FLUSH:
        	return new DiskCacheFlushTaskParameters();
        default:
            return null;
        }
    }

    /**
     * Creates a new instance of task-type specific parameter type.
     * static method for the same reason as above.
     */
    public static TaskRunner instantiateRunner (TaskType type) {
        switch (type) {
        case PARTITION_RAW_TEXT_FILES:
            return new PartitionRawTextFilesTaskRunner();
        case LOAD_PARTITIONED_TEXT_FILES:
            return new LoadPartitionedTextFilesTaskRunner();
        case RECOVER_PARTITION_FROM_BUDDY:
            return new RecoverPartitionFromBuddyTaskRunner();
        case RECOVER_PARTITION_FROM_REPARTITIONED_FILES:
            return new RecoverPartitionFromRepartitionedFilesTaskRunner();
        case MERGE_PARTITION_SAME_SCHEME:
            return new MergePartitionSameSchemeTaskRunner();
        case DELETE_PARTITION_FILES:
            return new DeletePartitionFilesTaskRunner();
        case DELETE_TMP_FILES:
            return new DeleteTmpFilesTaskRunner();
        case REPARTITION:
        	return new RepartitionTaskRunner();
        case BENCHMARK_TPCH_Q1:
            return new BenchmarkTpchQ1TaskRunner();
        case BENCHMARK_TPCH_Q15_PLANA:
            return new BenchmarkTpchQ15PlanATaskRunner();
        case BENCHMARK_TPCH_Q15_PLANB:
            return new BenchmarkTpchQ15PlanBTaskRunner();
        case BENCHMARK_TPCH_Q17_PLANA:
            return new BenchmarkTpchQ17PlanATaskRunner();
        case BENCHMARK_TPCH_Q17_PLANB:
            return new BenchmarkTpchQ17PlanBTaskRunner();
        case BENCHMARK_TPCH_Q18_PLANA:
            return new BenchmarkTpchQ18PlanATaskRunner();
        case BENCHMARK_TPCH_Q18_PLANB:
            return new BenchmarkTpchQ18PlanBTaskRunner();
        case DISK_CACHE_FLUSH:
        	return new DiskCacheFlushTaskRunner();
        default:
            return null;
        }
    }
}
