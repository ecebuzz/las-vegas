package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Represents a logical definition of a table.
 * Each table can have an arbitrary number (but at least 1)
 * of physical replicas.
 */
@Entity
public class LVTable {
    /**
     * A unique (system-wide) ID of this table.
     */
    @PrimaryKey
    private int tableId;
    
    /**
     * The logical name of this table. Of course has to be unique.
     * Only some set of characters are allowed in table name (as defined in ANSI SQL). 
     */
    @SecondaryKey(name="IX_NAME", relate=Relationship.ONE_TO_ONE)
    private String name;

    /** current status of this table. */
    private LVTableStatus status;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Table-" + tableId + "(" + name + "): status=" + status;
    }

 // auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the a unique (system-wide) ID of this table.
     *
     * @return the a unique (system-wide) ID of this table
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Sets the a unique (system-wide) ID of this table.
     *
     * @param tableId the new a unique (system-wide) ID of this table
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the logical name of this table.
     *
     * @return the logical name of this table
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the logical name of this table.
     *
     * @param name the new logical name of this table
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the current status of this table.
     *
     * @return the current status of this table
     */
    public LVTableStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this table.
     *
     * @param status the new current status of this table
     */
    public void setStatus(LVTableStatus status) {
        this.status = status;
    }
}