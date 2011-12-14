package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Rack is a collection of nodes physically located together.
 */
@Entity
public class LVRack implements LVObject {
    public static final String IX_NAME = "IX_NAME";
    /**
     * A unique name of the rack. Should be the same string as the rack names in HDFS.
     */
    @SecondaryKey(name=IX_NAME, relate=Relationship.ONE_TO_ONE)
    private String name;

    /**
     * Unique ID of the rack.
     */
    @PrimaryKey
    private int rackId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return rackId;
    }
    
    /**
     * Status of the rack.
     */
    private RackStatus status;
    
    @Override
    public String toString() {
        return "Rack-" + rackId + " (Name=" + name
        + ", Status=" + status + ")";
    }

// auto-generated getters/setters (comments by JAutodoc)

    /**
     * Gets the a unique name of the rack.
     *
     * @return the a unique name of the rack
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the a unique name of the rack.
     *
     * @param name the new a unique name of the rack
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the unique ID of the rack.
     *
     * @return the unique ID of the rack
     */
    public int getRackId() {
        return rackId;
    }

    /**
     * Sets the unique ID of the rack.
     *
     * @param rackId the new unique ID of the rack
     */
    public void setRackId(int rackId) {
        this.rackId = rackId;
    }

    /**
     * Gets the status of the rack.
     *
     * @return the status of the rack
     */
    public RackStatus getStatus() {
        return status;
    }

    /**
     * Sets the status of the rack.
     *
     * @param status the new status of the rack
     */
    public void setStatus(RackStatus status) {
        this.status = status;
    }    
}
