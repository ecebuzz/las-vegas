package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;

/**
 * Sub task of a few jobs.
 * Physically delete files in specified partitions in the node.
 */
public final class DeletePartitionFilesTaskRunner extends DataTaskRunner<DeletePartitionFilesTaskParameters> {
    private static Logger LOG = Logger.getLogger(DeletePartitionFilesTaskRunner.class);
    private LVReplicaPartition[] partitions;
    private List<ColumnFileBundle> files;
    @Override
    protected String[] runDataTask() throws Exception {
        LOG.info("deleting files in " + parameters.getPartitionIds().length + " partitions..");
        prepareInputs ();
        LOG.info("deleting " + files.size() + " column files..");
        for (ColumnFileBundle file : files) {
            deleteAndLog(file.getDataFile());
            deleteAndLog(file.getDictionaryFile());
            deleteAndLog(file.getPositionFile());
            deleteAndLog(file.getValueFile());
        }
        LOG.info("done!");
        return new String[0];
    }
    private void deleteAndLog (VirtualFile file) throws IOException {
        if (file == null) {
            return;
        }
        boolean deleted = file.delete();
        if (!deleted) {
            LOG.warn("couldn't delete this file:" + file.getAbsolutePath() + ".");
        }
    }

    private void prepareInputs () throws Exception {
        partitions = new LVReplicaPartition[parameters.getPartitionIds().length];
        files = new ArrayList<ColumnFileBundle> ();
        for (int i = 0; i < parameters.getPartitionIds().length; ++i) {
            partitions[i] = context.metaRepo.getReplicaPartition(parameters.getPartitionIds()[i]);
            if (partitions[i] == null) {
                throw new IOException ("this partition ID doesn't exist:" + parameters.getPartitionIds()[i]);
            }
            if (partitions[i].getNodeId() != context.nodeId) {
                throw new IOException ("this partition doesn't belong to this node:" + partitions[i]);
            }
            
            for (LVColumnFile file : context.metaRepo.getAllColumnFilesByReplicaPartitionId(partitions[i].getPartitionId())) {
                files.add(new ColumnFileBundle(file));
            }
        }
    }
}
