package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Represents a database containing an arbitrary number of tables.
 */
@Entity
public class LVDatabase implements LVObject {
    /**
     * A unique (system-wide) ID of this database.
     */
    @PrimaryKey
    private int databaseId;

    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return databaseId;
    }   
    
    /** The Constant IX_NAME. */
    public static final String IX_NAME = "IX_NAME";

    /**
     * The name of this database. Of course has to be unique.
     * Only some set of characters are allowed in table name (as defined in ANSI SQL). 
     */
    @SecondaryKey(name=IX_NAME, relate=Relationship.ONE_TO_ONE)
    private String name;

    /** current status of this database. */
    private DatabaseStatus status;

    @Override
    public String toString() {
        return "Database-" + databaseId + "(" + name + "): status=" + status;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(name == null);
        if (name != null) {
            out.writeUTF(name);
        }
        out.writeInt(status == null ? DatabaseStatus.INVALID.ordinal() : status.ordinal());
        out.writeInt(databaseId);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        boolean isNameNull = in.readBoolean();
        if (isNameNull) {
            name = null;
        } else {
            name = in.readUTF();
        }
        status = DatabaseStatus.values()[in.readInt()];
        databaseId = in.readInt();
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVDatabase read (DataInput in) throws IOException {
        LVDatabase obj = new LVDatabase();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.DATABASE;
    }

    // auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the a unique (system-wide) ID of this database.
     *
     * @return the a unique (system-wide) ID of this database
     */
    public int getDatabaseId() {
        return databaseId;
    }

    /**
     * Sets the a unique (system-wide) ID of this database.
     *
     * @param databaseId the new a unique (system-wide) ID of this database
     */
    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }

    /**
     * Gets the name of this database.
     *
     * @return the name of this database
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this database.
     *
     * @param name the new name of this database
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the current status of this database.
     *
     * @return the current status of this database
     */
    public DatabaseStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this database.
     *
     * @param status the new current status of this database
     */
    public void setStatus(DatabaseStatus status) {
        this.status = status;
    }
}
