package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;

/**
 * The Class BenchmarkTpchQ15TaskParameters.
 *
 * @see TaskType#BENCHMARK_TPCH_Q15_PLANA
 */
public final class BenchmarkTpchQ15TaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new benchmark tpch q15 task parameters.
     */
    public BenchmarkTpchQ15TaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new benchmark tpch q15 task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public BenchmarkTpchQ15TaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new benchmark tpch q15 task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public BenchmarkTpchQ15TaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /** The supplier table id. only for Plan A. */
    private int supplierTableId;
    
    /** The lineitem table id. */
    private int lineitemTableId;
    /** ID of part table's LVReplicaPartition to process in this node. only for Plan A. */
    private int[] supplierPartitionIds;
    /** ID of lineitem table's LVReplicaPartition to process in this node. */
    private int[] lineitemPartitionIds;

    /** query parameter: [DATE] in the form of 19960101.*/
    private int date;
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        supplierTableId = in.readInt();
        lineitemTableId = in.readInt();
        supplierPartitionIds = readIntArray(in);
        lineitemPartitionIds = readIntArray(in);
        date = in.readInt();
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(supplierTableId);
        out.writeInt(lineitemTableId);
        writeIntArray(out, supplierPartitionIds);
        writeIntArray(out, lineitemPartitionIds);
        out.writeInt(date);
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
	 * Gets the supplier partition ids.
	 *
	 * @return the supplier partition ids
	 */
	public int[] getSupplierPartitionIds() {
		return supplierPartitionIds;
	}

	/**
	 * Sets the supplier partition ids.
	 *
	 * @param supplierPartitionIds the new supplier partition ids
	 */
	public void setSupplierPartitionIds(int[] supplierPartitionIds) {
		this.supplierPartitionIds = supplierPartitionIds;
	}

	/**
	 * Gets the lineitem partition ids.
	 *
	 * @return the lineitem partition ids
	 */
	public int[] getLineitemPartitionIds() {
		return lineitemPartitionIds;
	}

	/**
	 * Sets the lineitem partition ids.
	 *
	 * @param lineitemPartitionIds the new lineitem partition ids
	 */
	public void setLineitemPartitionIds(int[] lineitemPartitionIds) {
		this.lineitemPartitionIds = lineitemPartitionIds;
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
