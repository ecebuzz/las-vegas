package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * A sub-partition of {@link LVReplica}.
 * All columns files in each partition are located in the same node
 * to speed up tuple re-construction.
 * LVReplicaPartition is also a unit for recovery and replication.
 */
@Entity
public class LVReplicaPartition {
    /**
     * ID of the replica (Replicated Fracture) this replica partition belongs to.
     */
    @SecondaryKey(name="IX_REPLICA_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplica.class)
    private int replicaId;

    /**
     * Key range of the partitioning column.
     */
    @SecondaryKey(name="IX_RANGE_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaPartitionRange.class)
    private int rangeId;

    /**
     * A hack to create a composite secondary index on Replica-ID and Range-ID.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name="IX_REPLICA_RANGE_ID", relate=Relationship.MANY_TO_ONE)
    private CompositeIntKey replicaRangeId = new CompositeIntKey();
    /** getter sees the actual members. */
    public CompositeIntKey getReplicaRangeId() {
        replicaRangeId.setValue1(replicaId);
        replicaRangeId.setValue2(rangeId);
        return replicaRangeId;
    }
    /** dummy setter. */
    public void setReplicaRangeId(CompositeIntKey replicaRangeId) {}

    /**
     * Unique ID of this replica partition.
     */
    @PrimaryKey
    private int partitionId;
    
    /**
     * The number of tuples in this replica partition.
     */
    private long tupleCount;
    
    /**
     * Current status of this replica partition.
     */
    @SecondaryKey(name="IX_STATUS", relate=Relationship.MANY_TO_ONE)
    private LVReplicaPartitionStatus status;

    /**
     * URI of the current HDFS node that contains this replica partition.
     */
    @SecondaryKey(name="IX_CURRENT_HDFS_NODE", relate=Relationship.MANY_TO_ONE)
    private String currentHdfsNodeUri;

    /**
     * URI of the HDFS node that is trying to recovery this replica partition.
     * As soon as the recovery is done, it becomes the new currentHdfsNodeUri
     * and recoveryHdfsNodeUri will be set to an empty string.
     * recoveryHdfsNodeUri is empty as far as the replica partition is intact.
     */
    @SecondaryKey(name="IX_RECOVERY_HDFS_NODE", relate=Relationship.MANY_TO_ONE)
    private String recoveryHdfsNodeUri;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ReplicaPartition-" + partitionId + " (Replica=" + replicaId
            + ", Range=" + rangeId + ")"
            + " TupleCount=" + tupleCount
            + " Status=" + status
            + " currentHdfsNodeUri=" + currentHdfsNodeUri
            + " recoveryHdfsNodeUri=" + recoveryHdfsNodeUri
            ;
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
    }

    /**
     * Gets the key range of the partitioning column.
     *
     * @return the key range of the partitioning column
     */
    public int getRangeId() {
        return rangeId;
    }

    /**
     * Sets the key range of the partitioning column.
     *
     * @param rangeId the new key range of the partitioning column
     */
    public void setRangeId(int rangeId) {
        this.rangeId = rangeId;
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
     * Gets the number of tuples in this replica partition.
     *
     * @return the number of tuples in this replica partition
     */
    public long getTupleCount() {
        return tupleCount;
    }

    /**
     * Sets the number of tuples in this replica partition.
     *
     * @param tupleCount the new number of tuples in this replica partition
     */
    public void setTupleCount(long tupleCount) {
        this.tupleCount = tupleCount;
    }

    /**
     * Gets the current status of this replica partition.
     *
     * @return the current status of this replica partition
     */
    public LVReplicaPartitionStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this replica partition.
     *
     * @param status the new current status of this replica partition
     */
    public void setStatus(LVReplicaPartitionStatus status) {
        this.status = status;
    }

    /**
     * Gets the uRI of the current HDFS node that contains this replica partition.
     *
     * @return the uRI of the current HDFS node that contains this replica partition
     */
    public String getCurrentHdfsNodeUri() {
        return currentHdfsNodeUri;
    }

    /**
     * Sets the uRI of the current HDFS node that contains this replica partition.
     *
     * @param currentHdfsNodeUri the new uRI of the current HDFS node that contains this replica partition
     */
    public void setCurrentHdfsNodeUri(String currentHdfsNodeUri) {
        this.currentHdfsNodeUri = currentHdfsNodeUri;
    }

    /**
     * Gets the uRI of the HDFS node that is trying to recovery this replica partition.
     *
     * @return the uRI of the HDFS node that is trying to recovery this replica partition
     */
    public String getRecoveryHdfsNodeUri() {
        return recoveryHdfsNodeUri;
    }

    /**
     * Sets the uRI of the HDFS node that is trying to recovery this replica partition.
     *
     * @param recoveryHdfsNodeUri the new uRI of the HDFS node that is trying to recovery this replica partition
     */
    public void setRecoveryHdfsNodeUri(String recoveryHdfsNodeUri) {
        this.recoveryHdfsNodeUri = recoveryHdfsNodeUri;
    }
}
