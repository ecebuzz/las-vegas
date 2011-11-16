package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Logical scheme of a column.
 */
@Entity
public class LVTableColumn {
    /**
     * ID of the table this column belongs to.
     */
    @SecondaryKey(name="IX_TABLE_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVTable.class)
    private int tableId;
    
    /**
     * A unique (system-wide) ID of this column.
     */
    @PrimaryKey
    private int columnId;

    /**
     * The name of this column. Unique in this table.
     */
    private String name;
    
    /**
     * Data type of this column.
     * @see LVColumnType
     */
    private LVColumnType type;
    
    /**
     * Sequential order in this table (count from 0).
     * Unique in this table.
     */
    private int order;

    /** current status of this table. */
    private LVColumnStatus status;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Column-" + columnId + "(" + name + ") in Table-" + tableId
        + ", order=" + order + ", status=" + status;
    }

// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the table this column belongs to.
     *
     * @return the iD of the table this column belongs to
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Sets the iD of the table this column belongs to.
     *
     * @param tableId the new iD of the table this column belongs to
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the a unique (system-wide) ID of this column.
     *
     * @return the a unique (system-wide) ID of this column
     */
    public int getColumnId() {
        return columnId;
    }

    /**
     * Sets the a unique (system-wide) ID of this column.
     *
     * @param columnId the new a unique (system-wide) ID of this column
     */
    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    /**
     * Gets the name of this column.
     *
     * @return the name of this column
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this column.
     *
     * @param name the new name of this column
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the data type of this column.
     *
     * @return the data type of this column
     */
    public LVColumnType getType() {
        return type;
    }

    /**
     * Sets the data type of this column.
     *
     * @param type the new data type of this column
     */
    public void setType(LVColumnType type) {
        this.type = type;
    }

    /**
     * Gets the sequential order in this table (count from 0).
     *
     * @return the sequential order in this table (count from 0)
     */
    public int getOrder() {
        return order;
    }

    /**
     * Sets the sequential order in this table (count from 0).
     *
     * @param order the new sequential order in this table (count from 0)
     */
    public void setOrder(int order) {
        this.order = order;
    }

    /**
     * Gets the current status of this table.
     *
     * @return the current status of this table
     */
    public LVColumnStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this table.
     *
     * @param status the new current status of this table
     */
    public void setStatus(LVColumnStatus status) {
        this.status = status;
    }
}
