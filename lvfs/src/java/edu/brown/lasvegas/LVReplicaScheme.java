package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * A replica scheme that specifies the partitioning and sorting
 * to replicate a table.
 */
@Entity
public class LVReplicaScheme implements LVObject {
    public static final String IX_GROUP_ID = "IX_GROUP_ID";
    /**
     * ID of the replica group (partitioning scheme) this scheme belongs to.
     */
    @SecondaryKey(name=IX_GROUP_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaGroup.class)
    private int groupId;
    
    /**
     * A unique (system-wide) ID of this replica scheme.
     */
    @PrimaryKey
    private int schemeId;
    @Override
    public int getPrimaryKey() {
        return schemeId;
    }   
    /**
     * ID of the column used as the in-block-sort key in this replica scheme.
     */
    private int sortColumnId;

    /**
     * Each column's compression scheme.
     * The key is column ID. If the map does not have corresponding column ID, the column
     * is supposed to be not compressed (can happen after adding a column).
     */
    private HashMap<Integer, CompressionType> columnCompressionSchemes = new HashMap<Integer, CompressionType>();
    /**
     * Gets the compression scheme of specified column.
     * @param columnId ID of the column
     * @return the compression scheme of the column
     */
    public CompressionType getColumnCompressionScheme(int columnId) {
        CompressionType type = columnCompressionSchemes.get(columnId);
        if (type == null) {
            // if not explicitly registered, it's no-compression
            return CompressionType.NONE;
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(groupId);
        out.writeInt(schemeId);
        out.writeInt(sortColumnId);
        Object[] array = columnCompressionSchemes.entrySet().toArray();
        out.writeInt(array.length);
        for (int i = 0; i < array.length; ++i) {
            @SuppressWarnings("unchecked")
            Map.Entry<Integer, CompressionType> entry = (Map.Entry<Integer, CompressionType>) array[i];
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue().ordinal());
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        groupId = in.readInt();
        schemeId = in.readInt();
        sortColumnId = in.readInt();
        columnCompressionSchemes.clear();
        int count = in.readInt();
        for (int i = 0; i < count; ++i) {
            int key = in.readInt();
            int val = in.readInt();
            assert (!columnCompressionSchemes.containsKey(key));
            columnCompressionSchemes.put(key, CompressionType.values()[val]);
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVReplicaScheme read (DataInput in) throws IOException {
        LVReplicaScheme obj = new LVReplicaScheme();
        obj.readFields(in);
        return obj;
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
    public HashMap<Integer, CompressionType> getColumnCompressionSchemes() {
        return columnCompressionSchemes;
    }

    /**
     * Sets the each column's compression scheme.
     *
     * @param columnCompressionSchemes the new each column's compression scheme
     */
    public void setColumnCompressionSchemes(HashMap<Integer, CompressionType> columnCompressionSchemes) {
        this.columnCompressionSchemes = columnCompressionSchemes;
    }
}
