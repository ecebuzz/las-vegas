package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import edu.brown.lasvegas.util.CompositeIntKey;

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

    public static final String IX_SCHEME_FRACTURE_ID = "IX_SCHEME_FRACTURE_ID";
    /**
     * A hack to create a composite secondary index on Scheme-ID and Fracture-ID.
     * Don't get or set this directly. Only BDB-JE should access it.
     */
    @SecondaryKey(name=IX_SCHEME_FRACTURE_ID, relate=Relationship.MANY_TO_ONE)
    private CompositeIntKey schemeFractureId = new CompositeIntKey();
    public CompositeIntKey getSchemeFractureId() {
        return schemeFractureId;
    }
    private void syncSchemeFractureId() {
        schemeFractureId.setValue1(schemeId);
        schemeFractureId.setValue2(fractureId);
    }
    public void setSchemeFractureId(CompositeIntKey schemeFractureId) {
        this.schemeFractureId = schemeFractureId;
    }
    
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
     * ID of the sub-partition scheme this partition is based on.
     * Can be obtained from replicaId, but easier if we have this here too (de-normalization).
     */
    private int subPartitionSchemeId;

    /**
     * To string.
     *
     * @return the string
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Replica-" + replicaId + "(Scheme=" + schemeId + ", Fracture=" + fractureId + ") "
        + "status=" + status + ", subPartitionSchemeId=" + subPartitionSchemeId
        ;
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
        syncSchemeFractureId();
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
        syncSchemeFractureId();
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

    /**
     * Gets the iD of the sub-partition scheme this replica uses.
     *
     * @return the iD of the sub-partition scheme this replica uses
     */
    public int getSubPartitionSchemeId() {
        return subPartitionSchemeId;
    }

    /**
     * Sets the iD of the replica partition scheme this replica uses.
     *
     * @param subPartitionSchemeId the new iD of the sub-partition scheme this replica uses
     */
    public void setSubPartitionSchemeId(int subPartitionSchemeId) {
        this.subPartitionSchemeId = subPartitionSchemeId;
    }
}
