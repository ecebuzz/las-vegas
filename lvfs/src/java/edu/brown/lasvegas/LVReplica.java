package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Replica (Replicated Fracture) is a stored
 * table fracture with some replica scheme.
 * It is the basic access unit
 * and the recovery unit in this system.
 */
@Entity
public class LVReplica implements LVObject {
    public static final String IX_SCHEME_ID = "IX_SCHEME_ID";
    /**
     * ID of the scheme of this replica.
     */
    @SecondaryKey(name=IX_SCHEME_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaScheme.class)
    private int schemeId;

    public static final String IX_FRACTURE_ID = "IX_FRACTURE_ID";
    /**
     * ID of the fracture of this replica.
     */
    @SecondaryKey(name=IX_FRACTURE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVFracture.class)
    private int fractureId;

    /**
     * A unique (system-wide) ID of this replica.
     */
    @PrimaryKey
    private int replicaId;
    @Override
    public int getPrimaryKey() {
        return replicaId;
    }   
    public static final String IX_STATUS = "IX_STATUS";
    /**
     * Status of this replica.
     */
    @SecondaryKey(name=IX_STATUS, relate=Relationship.MANY_TO_ONE)
    private ReplicaStatus status;

    /**
     * To string.
     *
     * @return the string
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Replica-" + replicaId + "(Scheme=" + schemeId + ", Fracture=" + fractureId + ") "
        + "status=" + status
        ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fractureId);
        out.writeInt(replicaId);
        out.writeInt(schemeId);
        out.writeInt(status == null ? ReplicaStatus.INVALID.ordinal() : status.ordinal());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        fractureId = in.readInt();
        replicaId = in.readInt();
        schemeId = in.readInt();
        status = ReplicaStatus.values()[in.readInt()];
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVReplica read (DataInput in) throws IOException {
        LVReplica obj = new LVReplica();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.REPLICA;
    }
    
// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the scheme of this replica.
     *
     * @return the iD of the scheme of this replica
     */
    public int getSchemeId() {
        return schemeId;
    }

    /**
     * Sets the iD of the scheme of this replica.
     *
     * @param schemeId the new iD of the scheme of this replica
     */
    public void setSchemeId(int schemeId) {
        this.schemeId = schemeId;
    }

    /**
     * Gets the iD of the fracture of this replica.
     *
     * @return the iD of the fracture of this replica
     */
    public int getFractureId() {
        return fractureId;
    }

    /**
     * Sets the iD of the fracture of this replica.
     *
     * @param fractureId the new iD of the fracture of this replica
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }

    /**
     * Gets the a unique (system-wide) ID of this replica.
     *
     * @return the a unique (system-wide) ID of this replica
     */
    public int getReplicaId() {
        return replicaId;
    }

    /**
     * Sets the a unique (system-wide) ID of this replica.
     *
     * @param replicaId the new a unique (system-wide) ID of this replica
     */
    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }

    /**
     * Gets the status of this replica.
     *
     * @return the status of this replica
     */
    public ReplicaStatus getStatus() {
        return status;
    }

    /**
     * Sets the status of this replica.
     *
     * @param status the new status of this replica
     */
    public void setStatus(ReplicaStatus status) {
        this.status = status;
    }
}
