package edu.brown.lasvegas;

import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * A replica scheme that specifies the partitioning and sorting
 * to replicate a table.
 */
@Entity
public class LVReplicaScheme {
    /**
     * ID of the replica group (partitioning scheme) this scheme belongs to.
     */
    @SecondaryKey(name="IX_GROUP_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaGroup.class)
    private int groupId;
    
    /**
     * A unique (system-wide) ID of this replica scheme.
     */
    @PrimaryKey
    private int schemeId;

    /**
     * ID of the column used as the in-block-sort key in this replica scheme.
     */
    private int sortColumnId;

    /**
     * Each column's compression scheme.
     * The key is column ID. If the map does not have corresponding column ID, the column
     * is supposed to be not compressed (can happen after adding a column).
     */
    private HashMap<Integer, LVCompressionType> columnCompressionSchemes = new HashMap<Integer, LVCompressionType>();
    /**
     * Gets the compression scheme of specified column.
     * @param columnId ID of the column
     * @return the compression scheme of the column
     */
    public LVCompressionType getColumnCompressionScheme(int columnId) {
        LVCompressionType type = columnCompressionSchemes.get(columnId);
        if (type == null) {
            // if not explicitly registered, it's no-compression
            return LVCompressionType.NONE;
        } else {
            return type;
        }
    }
    
    /**
     * To string.
     *
     * @return the string
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ReplicaScheme-" + schemeId + " in Group-" + groupId
            + ", sortColumnId=" + sortColumnId + ", compressionSchemes=" + columnCompressionSchemes;
    }

// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the replica group (partitioning scheme) this scheme belongs to.
     *
     * @return the iD of the replica group (partitioning scheme) this scheme belongs to
     */
    public int getGroupId() {
        return groupId;
    }

    /**
     * Sets the iD of the replica group (partitioning scheme) this scheme belongs to.
     *
     * @param groupId the new iD of the replica group (partitioning scheme) this scheme belongs to
     */
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    /**
     * Gets the a unique (system-wide) ID of this replica scheme.
     *
     * @return the a unique (system-wide) ID of this replica scheme
     */
    public int getSchemeId() {
        return schemeId;
    }

    /**
     * Sets the a unique (system-wide) ID of this replica scheme.
     *
     * @param schemeId the new a unique (system-wide) ID of this replica scheme
     */
    public void setSchemeId(int schemeId) {
        this.schemeId = schemeId;
    }

    /**
     * Gets the iD of the column used as the in-block-sort key in this replica scheme.
     *
     * @return the iD of the column used as the in-block-sort key in this replica scheme
     */
    public int getSortColumnId() {
        return sortColumnId;
    }

    /**
     * Sets the iD of the column used as the in-block-sort key in this replica scheme.
     *
     * @param sortColumnId the new iD of the column used as the in-block-sort key in this replica scheme
     */
    public void setSortColumnId(int sortColumnId) {
        this.sortColumnId = sortColumnId;
    }

    /**
     * Gets the each column's compression scheme.
     *
     * @return the each column's compression scheme
     */
    public HashMap<Integer, LVCompressionType> getColumnCompressionSchemes() {
        return columnCompressionSchemes;
    }

    /**
     * Sets the each column's compression scheme.
     *
     * @param columnCompressionSchemes the new each column's compression scheme
     */
    public void setColumnCompressionSchemes(HashMap<Integer, LVCompressionType> columnCompressionSchemes) {
        this.columnCompressionSchemes = columnCompressionSchemes;
    }
}
