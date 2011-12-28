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
 * The recovery unit of a table.
 * <p>A Fracture is a conceptual partitioning which defines
 * the subset of tuples from the table.</p>
 * 
 * <p>Fractures are totally separated; all recovery and querying happen per fracture.
 * Technically, two fractures are two different tables which happen
 * to have the same scheme.</p>
 * 
 * <p>Each table has exactly one {@link LVColumn} whose {@link LVColumn#isFracturingColumn()} returns
 * true. Fracture is a partition over the column. As default, the epoch column is the fracturing key.
 * If the user wants to choose non-default fracturing key, s/he has to make sure the key
 * is a valid partitioning key; any subsequently imported fracture must not have
 * an overlapping value of the fracturing key.
 * A monotonically increasing column such as sequentially numbered ID key is a good candidate.</p>
 */
@Entity
public class LVFracture implements LVObject {
    
    /** The Constant IX_TABLE_ID. */
    public static final String IX_TABLE_ID = "IX_TABLE_ID";
    /**
     * ID of the table this fracture belongs to.
     */
    @SecondaryKey(name=IX_TABLE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVTable.class)
    private int tableId;
    
    /**
     * A unique (system-wide) ID of this fracture.
     */
    @PrimaryKey
    private int fractureId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return fractureId;
    }
    /**
     * The key range of the fracturing key in this fracture.
     */
    private ValueRange range;

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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fractureId);
        out.writeInt(tableId);
        out.writeLong(tupleCount);
        out.writeBoolean(range == null);
        if (range != null) {
            range.write(out);
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        fractureId = in.readInt();
        tableId = in.readInt();
        tupleCount = in.readLong();
        if (!in.readBoolean()) {
            range = ValueRange.read(in);
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVFracture read (DataInput in) throws IOException {
        LVFracture obj = new LVFracture();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.FRACTURE;
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
     * Gets the key range of the fracturing key in this fracture.
     *
     * @return the key range of the fracturing key in this fracture.
     */
    public ValueRange getRange() {
        return range;
    }

    /**
     * Sets the key range of the fracturing key in this fracture.
     *
     * @param range the key range of the fracturing key in this fracture.
     */
    public void setRange(ValueRange range) {
        this.range = range;
    }
}
