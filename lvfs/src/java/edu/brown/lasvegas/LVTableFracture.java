package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import edu.brown.lasvegas.util.ValueRange;

/**
 * The recovery unit of a table.
 * A Fracture is a conceptual partitioning which defines
 * the subset of tuples from the table.
 * 
 * Fractures are totally separated; all recovery and querying happen per fracture.
 * Technically, two fractures are two different tables which happen
 * to have the same scheme.
 */
@Entity
public class LVTableFracture {
    /**
     * ID of the table this fracture belongs to.
     */
    @SecondaryKey(name="IX_TABLE_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVTable.class)
    private int tableId;
    
    /**
     * A unique (system-wide) ID of this fracture.
     */
    @PrimaryKey
    private int fractureId;

    /**
     * The key range of the base group's partitioning column in this fracture.
     * Could be tentatively NULL while creating a new fracture
     * if the base group uses automatic-epoch partitioning.
     */
    private ValueRange<?> range;

    /**
     * The number of tuples in this fracture.
     * This is just a statistics. Might not be accurate.
     */
    private long tupleCount;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Fracture-" + fractureId + " in Table-" + tableId
        + ": range=" + range
        + ". tupleCount=" + tupleCount;
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
     * Gets the a unique (system-wide) ID of this fracture.
     *
     * @return the a unique (system-wide) ID of this fracture
     */
    public int getFractureId() {
        return fractureId;
    }

    /**
     * Sets the a unique (system-wide) ID of this fracture.
     *
     * @param fractureId the new a unique (system-wide) ID of this fracture
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }

    /**
     * Gets the number of tuples in this fracture.
     *
     * @return the number of tuples in this fracture
     */
    public long getTupleCount() {
        return tupleCount;
    }

    /**
     * Sets the number of tuples in this fracture.
     *
     * @param tupleCount the new number of tuples in this fracture
     */
    public void setTupleCount(long tupleCount) {
        this.tupleCount = tupleCount;
    }

    /**
     * Gets the key range of the base group's partitioning column in this fracture.
     *
     * @return the key range of the base group's partitioning column in this fracture
     */
    public ValueRange<?> getRange() {
        return range;
    }

    /**
     * Sets the key range of the base group's partitioning column in this fracture.
     *
     * @param range the new key range of the base group's partitioning column in this fracture
     */
    public void setRange(ValueRange<?> range) {
        this.range = range;
    }
    
}
