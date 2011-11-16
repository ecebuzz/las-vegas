package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * The key range of the partitioning column in a fracture.
 * The same partition ranges are shared between the replica schemes
 * in the same replica group. So, such sibling replica schemes
 * can efficiently recover files between them.
 */
@Entity
public class LVReplicaPartitionRange {
    /**
     * ID of the fracture this partition range belongs to.
     */
    @SecondaryKey(name="IX_FRACTURE_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVTableFracture.class)
    private int fractureId;

    /**
     * ID of the replica group among which these partition ranges are shared.
     */
    @SecondaryKey(name="IX_GROUP_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaGroup.class)
    private int groupId;
    
    /**
     * A hack to create a composite secondary index on Fracture-ID and Group-ID.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name="IX_FRACTURE_GROUP_ID", relate=Relationship.MANY_TO_ONE)
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
    private int partitionRangeId;

    /**
     * The key range of the partitioning column in this group.
     */
    private ValueRange range;

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ReplicaPartitionRange-" + partitionRangeId
            + "(Fracture=" + fractureId + ", Group=" + groupId + ")"
            + " range=" + range;
    }
    
    
 // auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the fracture this partition range belongs to.
     *
     * @return the iD of the fracture this partition range belongs to
     */
    public int getFractureId() {
        return fractureId;
    }

    /**
     * Sets the iD of the fracture this partition range belongs to.
     *
     * @param fractureId the new iD of the fracture this partition range belongs to
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }

    /**
     * Gets the iD of the replica group among which these partition ranges are shared.
     *
     * @return the iD of the replica group among which these partition ranges are shared
     */
    public int getGroupId() {
        return groupId;
    }

    /**
     * Sets the iD of the replica group among which these partition ranges are shared.
     *
     * @param groupId the new iD of the replica group among which these partition ranges are shared
     */
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    /**
     * Gets the unique ID of this range.
     *
     * @return the unique ID of this range
     */
    public int getPartitionRangeId() {
        return partitionRangeId;
    }

    /**
     * Sets the unique ID of this range.
     *
     * @param partitionRangeId the new unique ID of this range
     */
    public void setPartitionRangeId(int partitionRangeId) {
        this.partitionRangeId = partitionRangeId;
    }

    /**
     * Gets the key range of the partitioning column in this group.
     *
     * @return the key range of the partitioning column in this group
     */
    public ValueRange getRange() {
        return range;
    }

    /**
     * Sets the key range of the partitioning column in this group.
     *
     * @param range the new key range of the partitioning column in this group
     */
    public void setRange(ValueRange range) {
        this.range = range;
    }    
}
