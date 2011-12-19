package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import edu.brown.lasvegas.util.CompositeIntKey;

/**
 * A sub-partition of {@link LVReplica}.
 * All column files in each partition are located in the same node
 * to speed up tuple re-construction.
 * LVReplicaPartition is also a unit for recovery and replication.
 */
@Entity
public class LVReplicaPartition implements LVObject {
    
    /** The Constant IX_REPLICA_ID. */
    public static final String IX_REPLICA_ID = "IX_REPLICA_ID";
    /**
     * ID of the replica (Replicated Fracture) this replica partition belongs to.
     */
    @SecondaryKey(name=IX_REPLICA_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplica.class)
    private int replicaId;

    /**
     * ID of the sub-partition scheme this partition is based on.
     * Can be obtained from replicaId, but easier if we have this here too (de-normalization).
     */
    private int subPartitionSchemeId;

    /**
     * The index in {@link LVSubPartitionScheme#getRanges()}.
     * Represents the key range this partition stores.
     */
    private int range;

    /** The Constant IX_REPLICA_RANGE. */
    public static final String IX_REPLICA_RANGE = "IX_REPLICA_RANGE";
    /**
     * A hack to create a composite secondary index on Replica-ID and Range.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name=IX_REPLICA_RANGE, relate=Relationship.MANY_TO_ONE)
    private CompositeIntKey replicaRange = new CompositeIntKey();
    
    /**
     * Gets the a hack to create a composite secondary index on Replica-ID and Range.
     *
     * @return the a hack to create a composite secondary index on Replica-ID and Range
     */
    public CompositeIntKey getReplicaRange() {
        return replicaRange;
    }
    
    /**
     * Sync replica range.
     */
    private void syncReplicaRange() {
        replicaRange.setValue1(replicaId);
        replicaRange.setValue2(range);
    }
    
    /**
     * Sets the a hack to create a composite secondary index on Replica-ID and Range.
     *
     * @param replicaRange the new a hack to create a composite secondary index on Replica-ID and Range
     */
    public void setReplicaRange(CompositeIntKey replicaRange) {}

    /**
     * Unique ID of this replica partition.
     */
    @PrimaryKey
    private int partitionId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return partitionId;
    }   
    
    /** The Constant IX_STATUS. */
    public static final String IX_STATUS = "IX_STATUS";
    /**
     * Current status of this replica partition.
     */
    @SecondaryKey(name=IX_STATUS, relate=Relationship.MANY_TO_ONE)
    private ReplicaPartitionStatus status;

    /** The Constant IX_NODE_ID. */
    public static final String IX_NODE_ID = "IX_NODE_ID";
    /**
     * The node that physically stores (will store) this replica partition.
     * This value could be NULL, in which case it means the replica partition
     * is not yet physically stored (or the node has been corrupted and it's being recovered).
     */
    @SecondaryKey(name=IX_NODE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVRackNode.class)
    private Integer nodeId;
    
    /**
     * To string.
     *
     * @return the string
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ReplicaPartition-" + partitionId + " (Replica=" + replicaId
            + ", Range=" + range + ")"
            + " subPartitionSchemeId=" + subPartitionSchemeId
            + " Status=" + status
            + " nodeId=" + nodeId
            ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeId == null ? -1 : nodeId);
        out.writeInt(partitionId);
        out.writeInt(range);
        out.writeInt(replicaId);
        out.writeInt(status.ordinal());
        out.writeInt(subPartitionSchemeId);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        nodeId = in.readInt();
        if (nodeId == -1) nodeId = null;
        partitionId = in.readInt();
        range = in.readInt();
        replicaId = in.readInt();
        status = ReplicaPartitionStatus.values()[in.readInt()];
        subPartitionSchemeId = in.readInt();
        syncReplicaRange();
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVReplicaPartition read (DataInput in) throws IOException {
        LVReplicaPartition obj = new LVReplicaPartition();
        obj.readFields(in);
        return obj;
    }

// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the replica (Replicated Fracture) this replica partition belongs to.
     *
     * @return the iD of the replica (Replicated Fracture) this replica partition belongs to
     */
    public int getReplicaId() {
        return replicaId;
    }

    /**
     * Sets the iD of the replica (Replicated Fracture) this replica partition belongs to.
     *
     * @param replicaId the new iD of the replica (Replicated Fracture) this replica partition belongs to
     */
    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
        syncReplicaRange();
    }

    /**
     * Gets the unique ID of this replica partition.
     *
     * @return the unique ID of this replica partition
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Sets the unique ID of this replica partition.
     *
     * @param partitionId the new unique ID of this replica partition
     */
    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * Gets the current status of this replica partition.
     *
     * @return the current status of this replica partition
     */
    public ReplicaPartitionStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this replica partition.
     *
     * @param status the new current status of this replica partition
     */
    public void setStatus(ReplicaPartitionStatus status) {
        this.status = status;
    }

    /**
     * Gets the index in {@link LVSubPartitionScheme#getRanges()}.
     *
     * @return the index in {@link LVSubPartitionScheme#getRanges()}
     */
    public int getRange() {
        return range;
    }
    
    /**
     * Sets the index in {@link LVSubPartitionScheme#getRanges()}.
     *
     * @param range the new index in {@link LVSubPartitionScheme#getRanges()}
     */
    public void setRange(int range) {
        this.range = range;
        syncReplicaRange();
    }

    /**
     * Gets the iD of the sub-partition scheme this partition is based on.
     *
     * @return the iD of the sub-partition scheme this partition is based on
     */
    public int getSubPartitionSchemeId() {
        return subPartitionSchemeId;
    }

    /**
     * Sets the iD of the sub-partition scheme this partition is based on.
     *
     * @param subPartitionSchemeId the new iD of the sub-partition scheme this partition is based on
     */
    public void setSubPartitionSchemeId(int subPartitionSchemeId) {
        this.subPartitionSchemeId = subPartitionSchemeId;
    }

    /**
     * Gets the node that physically stores (will store) this replica partition.
     *
     * @return the node that physically stores (will store) this replica partition
     */
    public Integer getNodeId() {
        return nodeId;
    }

    /**
     * Sets the node that physically stores (will store) this replica partition.
     *
     * @param nodeId the new node that physically stores (will store) this replica partition
     */
    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    
}
