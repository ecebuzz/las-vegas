package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.JobParameters;

/**
 * Parameters for {@link BenchmarkTpchQ17JobController}.
 */
public final class BenchmarkTpchQ17JobParameters extends JobParameters {
    /** ID of lineitem table. */
    private int lineitemTableId;
    /** ID of part table. */
    private int partTableId;
    
    /** query parameter: [BRAND].*/
    private String brand;
    /** query parameter: [CONTAINER].*/
    private String container;

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        lineitemTableId = in.readInt();
        partTableId = in.readInt();
        brand = in.readUTF();
        container = in.readUTF();
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(lineitemTableId);
        out.writeInt(partTableId);
        out.writeUTF(brand);
        out.writeUTF(container);
    }
    
    /**
     * Gets the iD of lineitem table.
     *
     * @return the iD of lineitem table
     */
    public int getLineitemTableId() {
        return lineitemTableId;
    }
    
    /**
     * Sets the iD of lineitem table.
     *
     * @param lineitemTableId the new iD of lineitem table
     */
    public void setLineitemTableId(int lineitemTableId) {
        this.lineitemTableId = lineitemTableId;
    }
    
    /**
     * Gets the iD of part table.
     *
     * @return the iD of part table
     */
    public int getPartTableId() {
        return partTableId;
    }
    
    /**
     * Sets the iD of part table.
     *
     * @param partTableId the new iD of part table
     */
    public void setPartTableId(int partTableId) {
        this.partTableId = partTableId;
    }

    /**
     * Gets the query parameter: [BRAND].
     *
     * @return the query parameter: [BRAND]
     */
    public String getBrand() {
        return brand;
    }

    /**
     * Sets the query parameter: [BRAND].
     *
     * @param brand the new query parameter: [BRAND]
     */
    public void setBrand(String brand) {
        this.brand = brand;
    }

    /**
     * Gets the query parameter: [CONTAINER].
     *
     * @return the query parameter: [CONTAINER]
     */
    public String getContainer() {
        return container;
    }

    /**
     * Sets the query parameter: [CONTAINER].
     *
     * @param container the new query parameter: [CONTAINER]
     */
    public void setContainer(String container) {
        this.container = container;
    }
}
