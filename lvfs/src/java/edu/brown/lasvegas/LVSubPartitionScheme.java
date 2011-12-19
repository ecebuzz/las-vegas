package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import edu.brown.lasvegas.util.ValueRange;

/**
 * Sub-partition scheme to define how to sub-partition
 * (in addition to fracture's partitioning) a replicated fracture.
 * The same sub-partition scheme is shared between the replica schemes
 * in the same replica group. So, such sibling replica schemes
 * can efficiently recover files between them.
 */
@Entity
public class LVSubPartitionScheme implements LVObject {
    
    /** The Constant IX_FRACTURE_ID. */
    public static final String IX_FRACTURE_ID = "IX_FRACTURE_ID";
    /**
     * ID of the fracture the sub-partitions belong to.
     */
    @SecondaryKey(name=IX_FRACTURE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVFracture.class)
    private int fractureId;

    /** The Constant IX_GROUP_ID. */
    public static final String IX_GROUP_ID = "IX_GROUP_ID";
    /**
     * ID of the replica group among which these sub-partitions are shared.
     */
    @SecondaryKey(name=IX_GROUP_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaGroup.class)
    private int groupId;
    
    /**
     * Unique ID of this sub-partition scheme.
     */
    @PrimaryKey
    private int subPartitionSchemeId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return subPartitionSchemeId;
    }   
    /** The type of the partitioning key. */
    private ColumnType keyType;
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
        buffer.append("SubPartitionScheme-" + subPartitionSchemeId)
            .append("(Fracture=" + fractureId + ", Group=" + groupId + ")");
        buffer.append(" ranges(keyType=" + keyType + ")={");
        for (ValueRange<?> range : ranges) {
            buffer.append(range + ",");
        }
        buffer.append("}");
        return new String(buffer);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fractureId);
        out.writeInt(groupId);
        out.writeInt(keyType.ordinal());
        ValueRange.writeRanges(out, ranges, keyType);
        out.writeInt(subPartitionSchemeId);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        fractureId = in.readInt();
        groupId = in.readInt();
        keyType = ColumnType.values()[in.readInt()];
        ranges = ValueRange.readRanges(in);
        subPartitionSchemeId = in.readInt();
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVSubPartitionScheme read (DataInput in) throws IOException {
        LVSubPartitionScheme obj = new LVSubPartitionScheme();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.SUB_PARTITION_SCHEME;
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
     * Gets the unique ID of this sub-partition scheme.
     *
     * @return the unique ID of this sub-partition scheme
     */
    public int getSubPartitionSchemeId() {
        return subPartitionSchemeId;
    }
    
    /**
     * Sets the unique ID of this sub-partition scheme.
     *
     * @param subPartitionSchemeId the new unique ID of this sub-partition scheme
     */
    public void setSubPartitionSchemeId(int subPartitionSchemeId) {
        this.subPartitionSchemeId = subPartitionSchemeId;
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

    /**
     * Gets the type of the partitioning key.
     *
     * @return the type of the partitioning key
     */
    public ColumnType getKeyType() {
        return keyType;
    }

    /**
     * Sets the type of the partitioning key.
     *
     * @param keyType the new type of the partitioning key
     */
    public void setKeyType(ColumnType keyType) {
        this.keyType = keyType;
    }
}
