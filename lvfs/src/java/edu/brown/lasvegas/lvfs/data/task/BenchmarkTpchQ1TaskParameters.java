package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;

/**
 * Parameters for BenchmarkTpchQ1TaskRunner.
 *
 * @see TaskType#BENCHMARK_TPCH_Q1
 */
public final class BenchmarkTpchQ1TaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new benchmark tpch q1 task parameters.
     */
    public BenchmarkTpchQ1TaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new benchmark tpch q1 task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public BenchmarkTpchQ1TaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new benchmark tpch q1 task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public BenchmarkTpchQ1TaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /** ID of lineitem table. */
    private int tableId;

    /** query parameter: [DELTA]. should be between 60 and 120. */
    private int deltaDays;

    /** ID of lineitem table's LVReplicaPartition to process in this node (potentially from multiple fractures). */
    private int[] partitionIds;
    
    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        tableId = in.readInt();
        deltaDays = in.readInt();
        partitionIds = readIntArray(in);
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tableId);
        out.writeInt(deltaDays);
        writeIntArray(out, partitionIds);
    }

    /**
     * Gets the iD of lineitem table.
     *
     * @return the iD of lineitem table
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Sets the iD of lineitem table.
     *
     * @param tableId the new iD of lineitem table
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the query parameter: [DELTA].
     *
     * @return the query parameter: [DELTA]
     */
    public int getDeltaDays() {
        return deltaDays;
    }

    /**
     * Sets the query parameter: [DELTA].
     *
     * @param deltaDays the new query parameter: [DELTA]
     */
    public void setDeltaDays(int deltaDays) {
        this.deltaDays = deltaDays;
    }

    /**
     * Gets the iD of lineitem table's LVReplicaPartition to process in this node (potentially from multiple fractures).
     *
     * @return the iD of lineitem table's LVReplicaPartition to process in this node (potentially from multiple fractures)
     */
    public int[] getPartitionIds() {
        return partitionIds;
    }

    /**
     * Sets the iD of lineitem table's LVReplicaPartition to process in this node (potentially from multiple fractures).
     *
     * @param partitionIds the new iD of lineitem table's LVReplicaPartition to process in this node (potentially from multiple fractures)
     */
    public void setPartitionIds(int[] partitionIds) {
        this.partitionIds = partitionIds;
    }
}
