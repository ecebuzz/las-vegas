package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;

/**
 * Parameters for  {@link DeletePartitionFilesTaskRunner}.
 */
public final class DeletePartitionFilesTaskParameters extends DataTaskParameters {
    public DeletePartitionFilesTaskParameters() {
        super();
    }
    public DeletePartitionFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public DeletePartitionFilesTaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /**
     * ID of LVReplicaPartition to delete.
     */
    private int[] partitionIds;
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(partitionIds == null ? -1 : partitionIds.length);
        if (partitionIds != null) {
            for (int i = 0; i < partitionIds.length; ++i) {
                out.writeInt(partitionIds[i]);
            }
        }
    }

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        assert (len >= -1);
        if (len == -1) {
            partitionIds = null;
        } else {
            partitionIds = new int[len];
            for (int i = 0; i < len; ++i) {
                partitionIds[i] = in.readInt();
            }
        }
    }
    
    /**
     * Gets the iD of LVReplicaPartition to delete.
     *
     * @return the iD of LVReplicaPartition to delete
     */
    public int[] getPartitionIds() {
        return partitionIds;
    }
    
    /**
     * Sets the iD of LVReplicaPartition to delete.
     *
     * @param partitionIds the new iD of LVReplicaPartition to delete
     */
    public void setPartitionIds(int[] partitionIds) {
        this.partitionIds = partitionIds;
    }
    
}
