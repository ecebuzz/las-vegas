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
 * A conceptual group of replica schemes which share the partitioning scheme.
 * Replica schemes in the same replica group can recover files
 * with minimal I/Os because they share the partitioning scheme.
 */
@Entity
public class LVReplicaGroup implements LVObject {
    public static final String IX_TABLE_ID = "IX_TABLE_ID";
    /**
     * ID of the table this fracture belongs to.
     */
    @SecondaryKey(name=IX_TABLE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVTable.class)
    private int tableId;
    
    /**
     * A unique (system-wide) ID of this replica group.
     */
    @PrimaryKey
    private int groupId;
    @Override
    public int getPrimaryKey() {
        return groupId;
    }   
    /**
     * ID of the column used as the partitioning key in this replica group. NULL if no partitioning.
     */
    private Integer partitioningColumnId;

    /**
     * The key ranges of the partitioning column in this replica group.
     * Sorted by the ranges themselves.
     */
    private ValueRange[] ranges;
    
    /**
     * ID of the replica group <b>in another table</b> this group is linked to (NULL if this group is independent).
     * If this group is linked to a group of another table, this group uses the same partitioning as another group
     * and corresponding partitions are co-located as much as possible to speed-up JOIN queries.
     * This property must not be changed after creation. So, no chance to have a cycle
     * (eg, this links to group-A, group-A links to group-B, group-B links to this group).
     */
    private Integer linkedGroupId;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("ReplicaGroup-" + groupId + " in Table-" + tableId
                        + " partitioning-column-id=" + partitioningColumnId + ", linkedGroupId=" + linkedGroupId);
        buffer.append(" ranges=");
        if (ranges == null) {
            buffer.append("null");
        } else {
            buffer.append("{");
            for (ValueRange range : ranges) {
                buffer.append(range + ",");
            }
            buffer.append("}");
        }
        return new String(buffer);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(groupId);
        out.writeInt(partitioningColumnId == null ? -1 : partitioningColumnId);
        out.writeInt(tableId);
        out.writeInt(ranges == null ? -1 : ranges.length);
        if (ranges != null) {
            for (ValueRange range : ranges) {
                if (range == null) {
                    range = new ValueRange(); // this will not happen, but let's make it sure
                }
                range.write(out);
            }
        }
        out.writeInt(linkedGroupId == null ? -1 : linkedGroupId.intValue());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        groupId = in.readInt();
        partitioningColumnId = in.readInt();
        if (partitioningColumnId < 0) {
            partitioningColumnId = null;
        }
        tableId = in.readInt();
        int len = in.readInt();
        if (len < 0) {
            ranges = null;
        } else {
            ranges = new ValueRange[len];
            for (int i = 0; i < len; ++i) {
                ranges[i] = ValueRange.read(in);
            }
        }
        linkedGroupId = in.readInt();
        if (linkedGroupId == -1) {
            linkedGroupId = null;
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVReplicaGroup read (DataInput in) throws IOException {
        LVReplicaGroup obj = new LVReplicaGroup();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.REPLICA_GROUP;
    }
// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the table this fracture belongs to.
     *
     * @return the iD of the table this fracture belongs to
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Sets the iD of the table this fracture belongs to.
     *
     * @param tableId the new iD of the table this fracture belongs to
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the a unique (system-wide) ID of this replica group.
     *
     * @return the a unique (system-wide) ID of this replica group
     */
    public int getGroupId() {
        return groupId;
    }

    /**
     * Sets the a unique (system-wide) ID of this replica group.
     *
     * @param groupId the new a unique (system-wide) ID of this replica group
     */
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    /**
     * Gets the iD of the replica group <b>in another table</b> this group is linked to (NULL if this group is independent).
     *
     * @return the iD of the replica group <b>in another table</b> this group is linked to (NULL if this group is independent)
     */
    public Integer getLinkedGroupId() {
        return linkedGroupId;
    }

    /**
     * Sets the iD of the replica group <b>in another table</b> this group is linked to (NULL if this group is independent).
     *
     * @param linkedGroupId the new iD of the replica group <b>in another table</b> this group is linked to (NULL if this group is independent)
     */
    public void setLinkedGroupId(Integer linkedGroupId) {
        this.linkedGroupId = linkedGroupId;
    }

    /**
     * Gets the key ranges of the partitioning column in this replica group.
     *
     * @return the key ranges of the partitioning column in this replica group
     */
    public ValueRange[] getRanges() {
        return ranges;
    }

    /**
     * Sets the key ranges of the partitioning column in this replica group.
     *
     * @param ranges the new key ranges of the partitioning column in this replica group
     */
    public void setRanges(ValueRange[] ranges) {
        this.ranges = ranges;
    }

    /**
     * Gets the iD of the column used as the partitioning key in this replica group.
     *
     * @return the iD of the column used as the partitioning key in this replica group
     */
    public Integer getPartitioningColumnId() {
        return partitioningColumnId;
    }

    /**
     * Sets the iD of the column used as the partitioning key in this replica group.
     *
     * @param partitioningColumnId the new iD of the column used as the partitioning key in this replica group
     */
    public void setPartitioningColumnId(Integer partitioningColumnId) {
        this.partitioningColumnId = partitioningColumnId;
    }
    
}
