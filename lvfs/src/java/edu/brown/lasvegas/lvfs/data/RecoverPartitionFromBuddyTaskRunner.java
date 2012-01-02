package edu.brown.lasvegas.lvfs.data;

import org.apache.log4j.Logger;

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
    private static Logger LOG = Logger.getLogger(RecoverPartitionFromBuddyTaskRunner.class);
    @Override
    protected String[] runDataTask() throws Exception {
        if (parameters.getPartitionIds().length == 0) {
            LOG.warn("no inputs for this node??");
            return new String[0];
        }
        LOG.info("recovering partitions from a buddy replica...");
        //TODO implement
        checkTaskCanceled();
        LOG.info("done!");
        return new String[0];
    }
}
