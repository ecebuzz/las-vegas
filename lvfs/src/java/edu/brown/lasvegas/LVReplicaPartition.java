package edu.brown.lasvegas;

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

    public static final String IX_REPLICA_RANGE = "IX_REPLICA_RANGE";
    /**
     * A hack to create a composite secondary index on Replica-ID and Range.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name=IX_REPLICA_RANGE, relate=Relationship.MANY_TO_ONE)
    private CompositeIntKey replicaRange = new CompositeIntKey();
    
    public CompositeIntKey getReplicaRange() {
        return replicaRange;
    }
    private void syncReplicaRange() {
        replicaRange.setValue1(replicaId);
        replicaRange.setValue2(range);
    }
    public void setReplicaRange(CompositeIntKey replicaRange) {}

    /**
     * Unique ID of this replica partition.
     */
    @PrimaryKey
    private int partitionId;
    @Override
    public int getPrimaryKey() {
        return partitionId;
    }   
    public static final String IX_STATUS = "IX_STATUS";
    /**
     * Current status of this replica partition.
     */
    @SecondaryKey(name=IX_STATUS, relate=Relationship.MANY_TO_ONE)
    private ReplicaPartitionStatus status;

    public static final String IX_CURRENT_HDFS_NODE = "IX_CURRENT_HDFS_NODE";
    /**
     * URI of the current HDFS node that contains this replica partition.
     */
    @SecondaryKey(name=IX_CURRENT_HDFS_NODE, relate=Relationship.MANY_TO_ONE)
    private String currentHdfsNodeUri;

    public static final String IX_RECOVERY_HDFS_NODE = "IX_RECOVERY_HDFS_NODE";
    /**
     * URI of the HDFS node that is trying to recovery this replica partition.
     * As soon as the recovery is done, it becomes the new currentHdfsNodeUri
     * and recoveryHdfsNodeUri will be set to an empty string.
     * recoveryHdfsNodeUri is empty as far as the replica partition is intact.
     */
    @SecondaryKey(name=IX_RECOVERY_HDFS_NODE, relate=Relationship.MANY_TO_ONE)
    private String recoveryHdfsNodeUri;
    
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
}
