package edu.brown.lasvegas.lvfs.data;


import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.TaskType;

/**
 * Sub task of {@link JobType#IMPORT_FRACTURE}.
 * Given one or more text files in the local filesystem (not HDFS),
 * this task partitions them into local temporary files.
 * This one is easier than PARTITION_HDFS_TEXT_FILES because
 * of the record-boundary issue in HDFS text files. 
 * @see TaskType#PARTITION_RAW_TEXT_FILES
 */
public class PartitionRawTextFilesTask {
}
