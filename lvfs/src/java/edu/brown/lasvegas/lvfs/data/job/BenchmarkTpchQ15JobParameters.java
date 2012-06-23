package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.JobParameters;

/**
 * Parameters for {@link BenchmarkTpchQ15JobController}.
 */
public final class BenchmarkTpchQ15JobParameters extends JobParameters {
    /** ID of lineitem table. */
    private int lineitemTableId;
    /** ID of supplier table. */
    private int supplierTableId;
    
    /** query parameter: [DATE] in the form of 19960101.*/
    private int date;

    /**
     * Read fields.
     *
     * @param in the in
     * @throws IOException Signals that an I/O exception has occurred.
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        lineitemTableId = in.readInt();
        supplierTableId = in.readInt();
        date = in.readInt();
    }
    
    /**
     * Write.
     *
     * @param out the out
     * @throws IOException Signals that an I/O exception has occurred.
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(lineitemTableId);
        out.writeInt(supplierTableId);
        out.writeInt(date);
    }

	/**
	 * Gets the lineitem table id.
	 *
	 * @return the lineitem table id
	 */
	public int getLineitemTableId() {
		return lineitemTableId;
	}

	/**
	 * Sets the lineitem table id.
	 *
	 * @param lineitemTableId the new lineitem table id
	 */
	public void setLineitemTableId(int lineitemTableId) {
		this.lineitemTableId = lineitemTableId;
	}

	/**
	 * Gets the supplier table id.
	 *
	 * @return the supplier table id
	 */
	public int getSupplierTableId() {
		return supplierTableId;
	}

	/**
	 * Sets the supplier table id.
	 *
	 * @param supplierTableId the new supplier table id
	 */
	public void setSupplierTableId(int supplierTableId) {
		this.supplierTableId = supplierTableId;
	}

	/**
	 * Gets the date.
	 *
	 * @return the date
	 */
	public int getDate() {
		return date;
	}

	/**
	 * Sets the date.
	 *
	 * @param date the new date
	 */
	public void setDate(int date) {
		this.date = date;
	}
}
