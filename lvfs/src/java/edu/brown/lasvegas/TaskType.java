package edu.brown.lasvegas;

import edu.brown.lasvegas.lvfs.data.LoadPartitionedTextFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.LoadPartitionedTextFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.PartitionRawTextFilesTaskRunner;
import edu.brown.lasvegas.lvfs.data.PartitionRawTextFilesTaskParameters;

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
     * @see LoadPartitionedTextFilesTaskRunner
     */
    LOAD_PARTITIONED_TEXT_FILES,
    
    
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
        default:
            return null;
        }
    }
}
