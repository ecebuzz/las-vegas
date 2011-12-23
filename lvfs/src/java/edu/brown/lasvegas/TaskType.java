package edu.brown.lasvegas;

import edu.brown.lasvegas.lvfs.data.PartitionRawTextFilesTask;

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
     * @see PartitionRawTextFilesTask
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
}
