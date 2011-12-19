package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
public class LVTable implements LVObject {
    /**
     * A unique (system-wide) ID of this table.
     */
    @PrimaryKey
    private int tableId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return tableId;
    }   
    
    /** The Constant IX_NAME. */
    public static final String IX_NAME = "IX_NAME";
    /**
     * The logical name of this table. Of course has to be unique.
     * Only some set of characters are allowed in table name (as defined in ANSI SQL). 
     */
    @SecondaryKey(name=IX_NAME, relate=Relationship.ONE_TO_ONE)
    private String name;

    /** current status of this table. */
    private TableStatus status;
    
    /**
     * The column used to fracture (partition) this table.
     * 
     * All replicas are technically partitioned by a composite partitioning,
     * fractures and sub-partitions.
     * Only one column in a table is a fracturing column.
     * Also, only the keys added as of table creation can be fracturing column.
     * As a default, epoch column is the fracturing column.
     */
    private int fracturingColumnId;
    
    /**
     * Whether all files of this table will be replicated to all nodes.
     * This flag is used for small tables to improve query runtime and recovery time.
     */
    private boolean pervasiveReplication;

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Table-" + tableId + "(" + name + "): status=" + status + ", fracturingColumnId=" + fracturingColumnId + ", pervasiveReplication=" + pervasiveReplication;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fracturingColumnId);
        out.writeUTF(name);
        out.writeBoolean(pervasiveReplication);
        out.writeInt(status.ordinal());
        out.writeInt(tableId);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        fracturingColumnId = in.readInt();
        name = in.readUTF();
        pervasiveReplication = in.readBoolean();
        status = TableStatus.values()[in.readInt()];
        tableId = in.readInt();
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVTable read (DataInput in) throws IOException {
        LVTable obj = new LVTable();
        obj.readFields(in);
        return obj;
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
    public TableStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this table.
     *
     * @param status the new current status of this table
     */
    public void setStatus(TableStatus status) {
        this.status = status;
    }

    /**
     * Gets the column used to fracture (partition) this table.
     *
     * @return the column used to fracture (partition) this table
     */
    public int getFracturingColumnId() {
        return fracturingColumnId;
    }

    /**
     * Sets the column used to fracture (partition) this table.
     *
     * @param fracturingColumnId the new column used to fracture (partition) this table
     */
    public void setFracturingColumnId(int fracturingColumnId) {
        this.fracturingColumnId = fracturingColumnId;
    }

    /**
     * Checks if is whether all files of this table will be replicated to all nodes.
     *
     * @return the whether all files of this table will be replicated to all nodes
     */
    public boolean isPervasiveReplication() {
        return pervasiveReplication;
    }

    /**
     * Sets the whether all files of this table will be replicated to all nodes.
     *
     * @param pervasiveReplication the new whether all files of this table will be replicated to all nodes
     */
    public void setPervasiveReplication(boolean pervasiveReplication) {
        this.pervasiveReplication = pervasiveReplication;
    }
}
