package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;
import edu.brown.lasvegas.util.DataInputOutputUtil;

/**
 * Parameters for BenchmarkTpchQ18TaskRunner.
 *
 * @see TaskType#BENCHMARK_TPCH_Q18
 */
public final class BenchmarkTpchQ18TaskParameters extends DataTaskParameters {
    
    public BenchmarkTpchQ18TaskParameters() {
        super();
    }
    
    public BenchmarkTpchQ18TaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public BenchmarkTpchQ18TaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /** ID of lineitem table. */
    private int lineitemTableId;
    /** ID of orders table. */
    private int ordersTableId;

    /** ID of orders table's LVReplicaPartition to process in this node. */
    private int[] ordersPartitionIds;
    /** ID of lineitem table's LVReplicaPartition to process in this node. */
    private int[] lineitemPartitionIds;

    /** query parameter: [QUANTITY]. should be between 312 and 315. */
    private double quantityThreshold;
    
    /**
     * Used only for query plan with repartitioning. key=nodeId, value=path of summary file.
     */
    private SortedMap<Integer, String> repartitionSummaryFileMap;
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	ordersTableId = in.readInt();
        lineitemTableId = in.readInt();
        ordersPartitionIds = readIntArray(in);
        lineitemPartitionIds = readIntArray(in);
        quantityThreshold = in.readDouble();
        repartitionSummaryFileMap = DataInputOutputUtil.readIntegerStringSortedMap(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(ordersTableId);
        out.writeInt(lineitemTableId);
        writeIntArray(out, ordersPartitionIds);
        writeIntArray(out, lineitemPartitionIds);
        out.writeDouble(quantityThreshold);
        DataInputOutputUtil.writeIntegerStringSortedMap(out, repartitionSummaryFileMap);
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
	 * Gets the orders table id.
	 *
	 * @return the orders table id
	 */
	public int getOrdersTableId() {
		return ordersTableId;
	}

	/**
	 * Sets the orders table id.
	 *
	 * @param ordersTableId the new orders table id
	 */
	public void setOrdersTableId(int ordersTableId) {
		this.ordersTableId = ordersTableId;
	}

	/**
	 * Gets the orders partition ids.
	 *
	 * @return the orders partition ids
	 */
	public int[] getOrdersPartitionIds() {
		return ordersPartitionIds;
	}

	/**
	 * Sets the orders partition ids.
	 *
	 * @param ordersPartitionIds the new orders partition ids
	 */
	public void setOrdersPartitionIds(int[] ordersPartitionIds) {
		this.ordersPartitionIds = ordersPartitionIds;
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
	 * Gets the quantity threshold.
	 *
	 * @return the quantity threshold
	 */
	public double getQuantityThreshold() {
		return quantityThreshold;
	}

	/**
	 * Sets the quantity threshold.
	 *
	 * @param quantityThreshold the new quantity threshold
	 */
	public void setQuantityThreshold(double quantityThreshold) {
		this.quantityThreshold = quantityThreshold;
	}

	/**
	 * Gets the repartition summary file map.
	 *
	 * @return the repartition summary file map
	 */
	public SortedMap<Integer, String> getRepartitionSummaryFileMap() {
		return repartitionSummaryFileMap;
	}

	/**
	 * Sets the repartition summary file map.
	 *
	 * @param repartitionSummaryFileMap the repartition summary file map
	 */
	public void setRepartitionSummaryFileMap(
			SortedMap<Integer, String> repartitionSummaryFileMap) {
		this.repartitionSummaryFileMap = repartitionSummaryFileMap;
	}
}
