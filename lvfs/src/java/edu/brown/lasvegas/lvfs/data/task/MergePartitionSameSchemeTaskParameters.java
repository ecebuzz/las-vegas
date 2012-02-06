package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;

/**
 * Parameters for  {@link MergePartitionSameSchemeTaskRunner}.
 */
public final class MergePartitionSameSchemeTaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new merge partition same scheme task parameters.
     */
    public MergePartitionSameSchemeTaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new merge partition same scheme task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MergePartitionSameSchemeTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new merge partition same scheme task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MergePartitionSameSchemeTaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /**
     * ID of LVReplicaPartition to be constructed.
     */
    private int newPartitionId;
    
    /**
     * ID of LVReplicaPartition to be based on.
     */
    private int[] basePartitionIds;
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(newPartitionId);
        out.writeInt(basePartitionIds == null ? -1 : basePartitionIds.length);
    }

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        newPartitionId = in.readInt();
        int len = in.readInt();
        assert (len >= -1);
        if (len == -1) {
            basePartitionIds = null;
        } else {
            basePartitionIds = new int[len];
            for (int i = 0; i < len; ++i) {
                basePartitionIds[i] = in.readInt();
            }
        }
    }
    
    // auto-generated getters/setters (comments by JAutodoc)    

    /**
     * Gets the iD of LVReplicaPartition to be constructed.
     *
     * @return the iD of LVReplicaPartition to be constructed
     */
    public int getNewPartitionId() {
        return newPartitionId;
    }
    
    /**
     * Sets the iD of LVReplicaPartition to be constructed.
     *
     * @param newPartitionId the new iD of LVReplicaPartition to be constructed
     */
    public void setNewPartitionId(int newPartitionId) {
        this.newPartitionId = newPartitionId;
    }
    
    /**
     * Gets the iD of LVReplicaPartition to be based on.
     *
     * @return the iD of LVReplicaPartition to be based on
     */
    public int[] getBasePartitionIds() {
        return basePartitionIds;
    }
    
    /**
     * Sets the iD of LVReplicaPartition to be based on.
     *
     * @param basePartitionIds the new iD of LVReplicaPartition to be based on
     */
    public void setBasePartitionIds(int[] basePartitionIds) {
        this.basePartitionIds = basePartitionIds;
    }
}
