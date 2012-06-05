package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.JobParameters;
import edu.brown.lasvegas.LVReplicaScheme;

/**
 * Parameters for {@link RecoverFractureForeignJobController}.
 */
public class RecoverFractureForeignJobParameters extends JobParameters {
    /** the fracture to restore. */
    private int fractureId;
    /** ID of {@link LVReplicaScheme} that is damaged and to be restored. */
    private int damagedSchemeId;
    /** ID of {@link LVReplicaScheme} that is not damaged and to be used for the recovery. */
    private int sourceSchemeId;
    
    public void readFields(DataInput in) throws IOException {
        fractureId = in.readInt();
        damagedSchemeId = in.readInt();
        sourceSchemeId = in.readInt();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fractureId);
        out.writeInt(damagedSchemeId);
        out.writeInt(sourceSchemeId);
    }

    /**
     * Gets the fracture to restore.
     *
     * @return the fracture to restore
     */
    public int getFractureId() {
        return fractureId;
    }
    
    /**
     * Sets the fracture to restore.
     *
     * @param fractureId the new fracture to restore
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }
    
    /**
     * Gets the iD of {@link LVReplicaScheme} that is damaged and to be restored.
     *
     * @return the iD of {@link LVReplicaScheme} that is damaged and to be restored
     */
    public int getDamagedSchemeId() {
        return damagedSchemeId;
    }
    
    /**
     * Sets the iD of {@link LVReplicaScheme} that is damaged and to be restored.
     *
     * @param damagedSchemeId the new iD of {@link LVReplicaScheme} that is damaged and to be restored
     */
    public void setDamagedSchemeId(int damagedSchemeId) {
        this.damagedSchemeId = damagedSchemeId;
    }
    
    /**
     * Gets the iD of {@link LVReplicaScheme} that is not damaged and to be used for the recovery.
     *
     * @return the iD of {@link LVReplicaScheme} that is not damaged and to be used for the recovery
     */
    public int getSourceSchemeId() {
        return sourceSchemeId;
    }
    
    /**
     * Sets the iD of {@link LVReplicaScheme} that is not damaged and to be used for the recovery.
     *
     * @param sourceSchemeId the new iD of {@link LVReplicaScheme} that is not damaged and to be used for the recovery
     */
    public void setSourceSchemeId(int sourceSchemeId) {
        this.sourceSchemeId = sourceSchemeId;
    }
    
}
