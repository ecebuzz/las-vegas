package edu.brown.lasvegas.lvfs.data;

import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.TaskType;

/**
 * Sub task of {@link JobType#IMPORT_FRACTURE} or recovery jobs such as {@link JobType#RECOVER_FRACTURE_FROM_BUDDY}.
 * Assuming a buddy (another replica scheme in the same replica group) has all
 * column files of a partition, this task reads, sorts, and compresses them to
 * its own column files. This task is supposed to be efficient because the communication will
 * be between nodes in the same rack.
 * @see TaskType#RECOVER_PARTITION_FROM_BUDDY
 */
public final class RecoverPartitionFromBuddyTaskRunner extends DataTaskRunner<RecoverPartitionFromBuddyTaskParameters>{
    @Override
    protected String[] runDataTask() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
}
