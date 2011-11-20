package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

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
     * ID of the column used as the partitioning key in this replica group.
     */
    private int partitioningColumnId;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ReplicaGroup-" + groupId + " in Table-" + tableId
        + " partitioning-column-id=" + partitioningColumnId;
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
     * Gets the iD of the column used as the partitioning key in this replica group.
     *
     * @return the iD of the column used as the partitioning key in this replica group
     */
    public int getPartitioningColumnId() {
        return partitioningColumnId;
    }

    /**
     * Sets the iD of the column used as the partitioning key in this replica group.
     *
     * @param partitioningColumnId the new iD of the column used as the partitioning key in this replica group
     */
    public void setPartitioningColumnId(int partitioningColumnId) {
        this.partitioningColumnId = partitioningColumnId;
    }
}
