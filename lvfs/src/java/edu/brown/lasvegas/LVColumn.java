package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Logical scheme of a column.
 */
@Entity
public class LVColumn implements LVObject {
    public LVColumn() {}
    public LVColumn(String name, ColumnType type) {
        this (name, type, false);
    }
    public LVColumn(String name, ColumnType type, boolean fracturingColumn) {
        setName(name);
        setType(type);
        setFracturingColumn(fracturingColumn);
    }
    
    public static final String IX_TABLE_ID = "IX_TABLE_ID";
    /**
     * ID of the table this column belongs to.
     */
    @SecondaryKey(name=IX_TABLE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVTable.class)
    private int tableId;
    
    /**
     * A unique (system-wide) ID of this column.
     */
    @PrimaryKey
    private int columnId;
    @Override
    public int getPrimaryKey() {
        return columnId;
    }

    /**
     * The name of this column. Unique in this table.
     */
    private String name;
    
    /**
     * epoch is an automatically added column to store coarse grained timestamp
     * of the tuple. The column always has this name and the is the first column (order=0). 
     */
    public static final String EPOCH_COLUMN_NAME = "__epoch";
    
    /**
     * Data type of this column.
     * @see ColumnType
     */
    private ColumnType type;
    
    /**
     * Sequential order in this table (usually from 1; 0 is always the "epoch" column).
     * Unique in this table.
     */
    private int order;

    /** current status of this table. */
    private ColumnStatus status;

    /**
     * Whether this column is used as table fracturing.
     * All replicas are technically partitioned by a composite partitioning,
     * fractures and sub-partitions.
     * Only one column in a table is a fracturing column.
     * Also, only the columns added as of table creation can be fracturing column.
     * As a default, epoch column is the fracturing column.
     */
    private boolean fracturingColumn;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Column-" + columnId + "(" + name + ") in Table-" + tableId
        + ", order=" + order + ", status=" + status + ",fracturingColumn?=" + fracturingColumn;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(columnId);
        out.writeBoolean(fracturingColumn);
        out.writeBoolean(name == null);
        if (name != null) {
            out.writeUTF(name);
        }
        out.writeInt(order);
        out.writeInt(status == null ? ColumnStatus.INVALID.ordinal() : status.ordinal());
        out.writeInt(tableId);
        out.writeInt(type == null ? ColumnType.INVALID.ordinal() : type.ordinal());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        columnId = in.readInt();
        fracturingColumn = in.readBoolean();
        boolean isNameNull = in.readBoolean();
        if (isNameNull) {
            name = null;
        } else {
            name = in.readUTF();
        }
        order = in.readInt();
        status = ColumnStatus.values()[in.readInt()];
        tableId = in.readInt();
        type = ColumnType.values()[in.readInt()];
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVColumn read (DataInput in) throws IOException {
        LVColumn obj = new LVColumn();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.COLUMN;
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
    public ColumnType getType() {
        return type;
    }

    /**
     * Sets the data type of this column.
     *
     * @param type the new data type of this column
     */
    public void setType(ColumnType type) {
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
    public ColumnStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this table.
     *
     * @param status the new current status of this table
     */
    public void setStatus(ColumnStatus status) {
        this.status = status;
    }

    /**
     * Checks if is whether this column is used as table fracturing.
     *
     * @return the whether this column is used as table fracturing
     */
    public boolean isFracturingColumn() {
        return fracturingColumn;
    }

    /**
     * Sets the whether this column is used as table fracturing.
     *
     * @param fracturingColumn the new whether this column is used as table fracturing
     */
    public void setFracturingColumn(boolean fracturingColumn) {
        this.fracturingColumn = fracturingColumn;
    }
}
