package edu.brown.lasvegas.lvfs.data;

import edu.brown.lasvegas.JobType;

/**
 * Sub task of {@link JobType#IMPORT_FRACTURE}.
 * Given partitioned text files output by {@link #PARTITION_TEXT_FILES} task,
 * this task collects those text files from local and remote nodes and
 * construct LVFS files in the local drive.
 */
public final class LoadPartitionedTextFilesTaskRunner extends DataTaskRunner<LoadPartitionedTextFilesTaskParameters> {
    @Override
    protected String[] runDataTask() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
}
