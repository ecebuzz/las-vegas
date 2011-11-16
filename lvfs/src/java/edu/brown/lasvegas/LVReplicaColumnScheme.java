package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Specifies how to compress a column file in each replica scheme.
 */
@Entity
public class LVReplicaColumnScheme {
    /**
     * ID of the replica scheme.
     */
    @SecondaryKey(name="IX_SCHEME_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaScheme.class)
    private int schemeId;
    
    /**
     * ID of the column.
     */
    private int columnId;

    /**
     * A unique (system-wide) ID of this column scheme.
     */
    @PrimaryKey
    private int columnSchemeId;
    
    /**
     * Compression scheme of the column files. 
     */
    private LVCompressionType compressionType;

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ReplicaColumnScheme-" + columnSchemeId + " (columnId=" + columnId
            + ", schemeId=" + schemeId + ") compression=" + compressionType;
    }

// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the replica scheme.
     *
     * @return the iD of the replica scheme
     */
    public int getSchemeId() {
        return schemeId;
    }

    /**
     * Sets the iD of the replica scheme.
     *
     * @param schemeId the new iD of the replica scheme
     */
    public void setSchemeId(int schemeId) {
        this.schemeId = schemeId;
    }

    /**
     * Gets the iD of the column.
     *
     * @return the iD of the column
     */
    public int getColumnId() {
        return columnId;
    }

    /**
     * Sets the iD of the column.
     *
     * @param columnId the new iD of the column
     */
    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    /**
     * Gets the a unique (system-wide) ID of this column scheme.
     *
     * @return the a unique (system-wide) ID of this column scheme
     */
    public int getColumnSchemeId() {
        return columnSchemeId;
    }

    /**
     * Sets the a unique (system-wide) ID of this column scheme.
     *
     * @param columnSchemeId the new a unique (system-wide) ID of this column scheme
     */
    public void setColumnSchemeId(int columnSchemeId) {
        this.columnSchemeId = columnSchemeId;
    }

    /**
     * Gets the compression scheme of the column files.
     *
     * @return the compression scheme of the column files
     */
    public LVCompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Sets the compression scheme of the column files.
     *
     * @param compressionType the new compression scheme of the column files
     */
    public void setCompressionType(LVCompressionType compressionType) {
        this.compressionType = compressionType;
    }
}
