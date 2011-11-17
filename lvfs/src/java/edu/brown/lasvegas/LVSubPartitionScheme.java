package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import edu.brown.lasvegas.util.CompositeIntKey;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Sub-partition scheme to define how to sub-partition
 * (in addition to fracture's partitioning) a replicated fracture.
 * The same sub-partition scheme is shared between the replica schemes
 * in the same replica group. So, such sibling replica schemes
 * can efficiently recover files between them.
 */
@Entity
public class LVSubPartitionScheme {
    public static final String IX_FRACTURE_ID = "IX_FRACTURE_ID";
    /**
     * ID of the fracture the sub-partitions belong to.
     */
    @SecondaryKey(name=IX_FRACTURE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVFracture.class)
    private int fractureId;

    public static final String IX_GROUP_ID = "IX_GROUP_ID";
    /**
     * ID of the replica group among which these sub-partitions are shared.
     */
    @SecondaryKey(name=IX_GROUP_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaGroup.class)
    private int groupId;
    
    public static final String IX_FRACTURE_GROUP_ID = "IX_FRACTURE_GROUP_ID";
    /**
     * A hack to create a composite secondary index on Fracture-ID and Group-ID.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name=IX_FRACTURE_GROUP_ID, relate=Relationship.MANY_TO_ONE)
    private CompositeIntKey fractureGroupId = new CompositeIntKey();
    
    /** getter sees the actual members. */
    public CompositeIntKey getFractureGroupId() {
        fractureGroupId.setValue1(fractureId);
        fractureGroupId.setValue2(groupId);
        return fractureGroupId;
    }
    
    /** dummy setter. */
    public void setFractureGroupId(CompositeIntKey fractureGroupId) {}
    
    /**
     * Unique ID of this range.
     */
    @PrimaryKey
    private int replicaPartitionSchemeId;

    /**
     * The key ranges of the partitioning column in this group.
     * Sorted by the ranges themselves.
     */
    private ValueRange<?>[] ranges;

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("ReplicaPartitionScheme-" + replicaPartitionSchemeId)
            .append("(Fracture=" + fractureId + ", Group=" + groupId + ")");
        buffer.append(" ranges={");
        for (ValueRange<?> range : ranges) {
            buffer.append(range + ",");
        }
        buffer.append("}");
        return new String(buffer);
    }

    // auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the fracture the sub-partitions belong to.
     *
     * @return the iD of the fracture the sub-partitions belong to
     */
    public int getFractureId() {
        return fractureId;
    }
    
    /**
     * Sets the iD of the fracture the sub-partitions belong to.
     *
     * @param fractureId the new iD of the fracture the sub-partitions belong to
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }
    
    /**
     * Gets the iD of the replica group among which these sub-partitions are shared.
     *
     * @return the iD of the replica group among which these sub-partitions are shared
     */
    public int getGroupId() {
        return groupId;
    }
    
    /**
     * Sets the iD of the replica group among which these sub-partitions are shared.
     *
     * @param groupId the new iD of the replica group among which these sub-partitions are shared
     */
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }
    
    /**
     * Gets the unique ID of this range.
     *
     * @return the unique ID of this range
     */
    public int getReplicaPartitionSchemeId() {
        return replicaPartitionSchemeId;
    }
    
    /**
     * Sets the unique ID of this range.
     *
     * @param replicaPartitionSchemeId the new unique ID of this range
     */
    public void setReplicaPartitionSchemeId(int replicaPartitionSchemeId) {
        this.replicaPartitionSchemeId = replicaPartitionSchemeId;
    }
    
    /**
     * Gets the key ranges of the partitioning column in this group.
     *
     * @return the key ranges of the partitioning column in this group
     */
    public ValueRange<?>[] getRanges() {
        return ranges;
    }
    
    /**
     * Sets the key ranges of the partitioning column in this group.
     *
     * @param ranges the new key ranges of the partitioning column in this group
     */
    public void setRanges(ValueRange<?>[] ranges) {
        this.ranges = ranges;
    }
}
