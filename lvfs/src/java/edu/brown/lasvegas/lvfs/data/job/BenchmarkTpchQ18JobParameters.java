package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.JobParameters;

/**
 * Query parameters for TPCH Q18.
 */
public final class BenchmarkTpchQ18JobParameters extends JobParameters {
    /** ID of lineitem table. */
    private int lineitemTableId;
    /** ID of orders table. */
    private int ordersTableId;
    /** ID of customer table. */
    private int customerTableId;
    
    /** query parameter: [QUANTITY]. should be between 312 and 315. */
    private double quantityThreshold;

    @Override
    public void readFields(DataInput in) throws IOException {
    	lineitemTableId = in.readInt();
    	ordersTableId = in.readInt();
    	customerTableId = in.readInt();
    	quantityThreshold = in.readDouble();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(lineitemTableId);
    	out.writeInt(ordersTableId);
    	out.writeInt(customerTableId);
    	out.writeDouble(quantityThreshold);
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
	 * Gets the customer table id.
	 *
	 * @return the customer table id
	 */
	public int getCustomerTableId() {
		return customerTableId;
	}

	/**
	 * Sets the customer table id.
	 *
	 * @param customerTableId the new customer table id
	 */
	public void setCustomerTableId(int customerTableId) {
		this.customerTableId = customerTableId;
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
}
