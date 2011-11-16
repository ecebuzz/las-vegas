package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
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
public class LVReplica {
    /**
     * ID of the scheme of this replica.
     */
    @SecondaryKey(name="IX_SCHEME_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaScheme.class)
    private int schemeId;

    /**
     * ID of the fracture of this replica.
     */
    @SecondaryKey(name="IX_FRACTURE_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVTableFracture.class)
    private int fractureId;

    /** composite class to create a composite secondary index. */
    @Persistent
    public static class SchemeFractureId {
        /** The scheme id. */
        @KeyField(1)
        int schemeId;
        
        /** The fracture id. */
        @KeyField(2)
        int fractureId;
    }
    
    /**
     * A hack to create a composite secondary index on Scheme-ID and Fracture-ID.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name="IX_SCHEME_FRACTURE_ID", relate=Relationship.MANY_TO_ONE)
    private SchemeFractureId schemeFractureId;
    
    /**
     * A unique (system-wide) ID of this replica.
     */
    @PrimaryKey
    private int replicaId;
    
    /**
     * Status of this replica.
     */
    private LVReplicaStatus status;

    /**
     * To string.
     *
     * @return the string
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Replica-" + replicaId + "(Scheme=" + schemeId + ", Fracture=" + fractureId + ") status=" + status;
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
     * Gets the a hack to create a composite secondary index on Scheme-ID and Fracture-ID.
     *
     * @return the a hack to create a composite secondary index on Scheme-ID and Fracture-ID
     */
    public SchemeFractureId getSchemeFractureId() {
        return schemeFractureId;
    }

    /**
     * Sets the a hack to create a composite secondary index on Scheme-ID and Fracture-ID.
     *
     * @param schemeFractureId the new a hack to create a composite secondary index on Scheme-ID and Fracture-ID
     */
    public void setSchemeFractureId(SchemeFractureId schemeFractureId) {
        this.schemeFractureId = schemeFractureId;
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
    public LVReplicaStatus getStatus() {
        return status;
    }

    /**
     * Sets the status of this replica.
     *
     * @param status the new status of this replica
     */
    public void setStatus(LVReplicaStatus status) {
        this.status = status;
    }
}
