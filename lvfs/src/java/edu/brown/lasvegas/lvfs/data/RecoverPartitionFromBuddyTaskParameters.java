package edu.brown.lasvegas.lvfs.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTask;

/**
 * Parameters for {@link RecoverPartitionFromBuddyTaskRunner}.
 */
public final class RecoverPartitionFromBuddyTaskParameters extends DataTaskParameters {
    public RecoverPartitionFromBuddyTaskParameters() {
        super();
    }
    public RecoverPartitionFromBuddyTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public RecoverPartitionFromBuddyTaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /**
     * ID of the replica scheme ({@link LVReplicaScheme}) to be recovered at this data node.
     */
    private int replicaSchemeId;
    
    /**
     * ID of the replica scheme ({@link LVReplicaScheme}) to provide the buddy files for this recovery.
     */
    private int buddyReplicaSchemeId;
    
    /**
     * ID of the partitions ({@link LVReplicaPartition}) to be recovered at this data node.
     */
    private int[] partitionIds;

    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(replicaSchemeId);
        out.writeInt(buddyReplicaSchemeId);
        out.writeInt(partitionIds == null ? -1 : partitionIds.length);
        if (partitionIds != null)
        for (int partitionId : partitionIds) {
            out.writeInt(partitionId);
        }
    }

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        replicaSchemeId = in.readInt();
        buddyReplicaSchemeId = in.readInt();
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
    
// auto-generated getters/setters (comments by JAutodoc)    
    /**
     * Gets the iD of the replica scheme ({@link LVReplicaScheme}) to be recovered at this data node.
     *
     * @return the iD of the replica scheme ({@link LVReplicaScheme}) to be recovered at this data node
     */
    public int getReplicaSchemeId() {
        return replicaSchemeId;
    }
    
    /**
     * Sets the iD of the replica scheme ({@link LVReplicaScheme}) to be recovered at this data node.
     *
     * @param replicaSchemeId the new iD of the replica scheme ({@link LVReplicaScheme}) to be recovered at this data node
     */
    public void setReplicaSchemeId(int replicaSchemeId) {
        this.replicaSchemeId = replicaSchemeId;
    }
    
    /**
     * Gets the iD of the replica scheme ({@link LVReplicaScheme}) to provide the buddy files for this recovery.
     *
     * @return the iD of the replica scheme ({@link LVReplicaScheme}) to provide the buddy files for this recovery
     */
    public int getBuddyReplicaSchemeId() {
        return buddyReplicaSchemeId;
    }
    
    /**
     * Sets the iD of the replica scheme ({@link LVReplicaScheme}) to provide the buddy files for this recovery.
     *
     * @param buddyReplicaSchemeId the new iD of the replica scheme ({@link LVReplicaScheme}) to provide the buddy files for this recovery
     */
    public void setBuddyReplicaSchemeId(int buddyReplicaSchemeId) {
        this.buddyReplicaSchemeId = buddyReplicaSchemeId;
    }
    
    /**
     * Gets the iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node.
     *
     * @return the iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node
     */
    public int[] getPartitionIds() {
        return partitionIds;
    }
    
    /**
     * Sets the iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node.
     *
     * @param partitionIds the new iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node
     */
    public void setPartitionIds(int[] partitionIds) {
        this.partitionIds = partitionIds;
    }
}
