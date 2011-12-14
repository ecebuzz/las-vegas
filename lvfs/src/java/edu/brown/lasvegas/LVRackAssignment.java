package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Each rack is assigned to only one {@link LVReplicaGroup} for
 * each fracture to simplify the recovery and maximize its performance.
 * <p>In other words, a replica group <b>exclusively owns</b> one or more racks
 * regarding a fracture.</p>
 */
@Entity
public class LVRackAssignment implements LVObject {
    /** unique ID of the assignment. */
    @PrimaryKey
    private int assignmentId;
    
    @Override
    public int getPrimaryKey() {
        return assignmentId;
    }
    
    /** The Constant IX_FRACTURE_ID. */
    public static final String IX_FRACTURE_ID = "IX_FRACTURE_ID";

    /** The fracture this assignment regards to. */
    @SecondaryKey(name=IX_FRACTURE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVFracture.class)
    private int fractureId;
    
    /** The Constant IX_RACK_ID. */
    public static final String IX_RACK_ID = "IX_RACK_ID";

    /** The rack this assignment is about. */
    @SecondaryKey(name=IX_RACK_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVRack.class)
    private int rackId;
    
    /** The Constant IX_OWNER. */
    public static final String IX_OWNER = "IX_OWNER";

    /** The replica group that owns the rack regarding the fracture. */
    @SecondaryKey(name=IX_OWNER, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaGroup.class)
    private int ownerReplicaGroupId;
    
    
    @Override
    public String toString() {
        return "Assignment-" + assignmentId + " of Rack-" + rackId + " on Fracture-" + fractureId + ": owner=ReplicaGroup-" + ownerReplicaGroupId;
    }

 // auto-generated getters/setters (comments by JAutodoc)

    /**
     * Gets the unique ID of the assignment.
     *
     * @return the unique ID of the assignment
     */
    public int getAssignmentId() {
        return assignmentId;
    }


    /**
     * Sets the unique ID of the assignment.
     *
     * @param assignmentId the new unique ID of the assignment
     */
    public void setAssignmentId(int assignmentId) {
        this.assignmentId = assignmentId;
    }


    /**
     * Gets the fracture this assignment regards to.
     *
     * @return the fracture this assignment regards to
     */
    public int getFractureId() {
        return fractureId;
    }


    /**
     * Sets the fracture this assignment regards to.
     *
     * @param fractureId the new fracture this assignment regards to
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }


    /**
     * Gets the rack this assignment is about.
     *
     * @return the rack this assignment is about
     */
    public int getRackId() {
        return rackId;
    }


    /**
     * Sets the rack this assignment is about.
     *
     * @param rackId the new rack this assignment is about
     */
    public void setRackId(int rackId) {
        this.rackId = rackId;
    }


    /**
     * Gets the replica group that owns the rack regarding the fracture.
     *
     * @return the replica group that owns the rack regarding the fracture
     */
    public int getOwnerReplicaGroupId() {
        return ownerReplicaGroupId;
    }


    /**
     * Sets the replica group that owns the rack regarding the fracture.
     *
     * @param ownerReplicaGroupId the new replica group that owns the rack regarding the fracture
     */
    public void setOwnerReplicaGroupId(int ownerReplicaGroupId) {
        this.ownerReplicaGroupId = ownerReplicaGroupId;
    }
}
