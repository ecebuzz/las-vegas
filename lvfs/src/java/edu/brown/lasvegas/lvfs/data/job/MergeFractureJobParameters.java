package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.JobParameters;

/**
 * Parameters for merging multiple fractures into one.
 */
public final class MergeFractureJobParameters extends JobParameters {
    /** ID of the fractures to be merged. */
    private int[] fractureIds;
    
    /**
     * Instantiates a new merge fracture job parameters.
     */
    public MergeFractureJobParameters() {}
    
    /**
     * Instantiates a new merge fracture job parameters.
     *
     * @param fractureIds the fracture ids
     */
    public MergeFractureJobParameters(int[] fractureIds) {
        this.fractureIds = fractureIds;
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            fractureIds = null;
        } else {
            fractureIds = new int[length];
            for (int i = 0; i < length; ++i) {
                fractureIds[i] = in.readInt();
            }
        }
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fractureIds == null ? -1 : fractureIds.length);
        if (fractureIds != null) {
            for (int i = 0; i < fractureIds.length; ++i) {
                out.writeInt(fractureIds[i]);
            }
        }
    }

    /**
     * Gets the iD of the fractures to be merged.
     *
     * @return the iD of the fractures to be merged
     */
    public int[] getFractureIds() {
        return fractureIds;
    }

    /**
     * Sets the iD of the fractures to be merged.
     *
     * @param fractureIds the new iD of the fractures to be merged
     */
    public void setFractureIds(int[] fractureIds) {
        this.fractureIds = fractureIds;
    }
}
