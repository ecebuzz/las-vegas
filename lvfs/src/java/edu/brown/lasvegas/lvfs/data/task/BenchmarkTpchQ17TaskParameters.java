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
 * The Class BenchmarkTpchQ17TaskParameters.
 *
 * @see TaskType#BENCHMARK_TPCH_Q17_PLANA
 */
public final class BenchmarkTpchQ17TaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new benchmark tpch q17 task parameters.
     */
    public BenchmarkTpchQ17TaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new benchmark tpch q17 task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public BenchmarkTpchQ17TaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new benchmark tpch q17 task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public BenchmarkTpchQ17TaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /** The part table id. */
    private int partTableId;
    
    /** The lineitem table id. */
    private int lineitemTableId;
    /** ID of part table's LVReplicaPartition to process in this node. */
    private int[] partPartitionIds;
    /** ID of lineitem table's LVReplicaPartition to process in this node. */
    private int[] lineitemPartitionIds;

    /** query parameter: [BRAND].*/
    private String brand;
    /** query parameter: [CONTAINER].*/
    private String container;
    
    /**
     * Used only for query plan with repartitioning. key=nodeId, value=path of summary file.
     */
    private SortedMap<Integer, String> repartitionSummaryFileMap;
    
    /**
     * Read fields.
     *
     * @param in the in
     * @throws IOException Signals that an I/O exception has occurred.
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        partTableId = in.readInt();
        lineitemTableId = in.readInt();
        partPartitionIds = readIntArray(in);
        lineitemPartitionIds = readIntArray(in);
        brand = in.readUTF();
        container = in.readUTF();
        repartitionSummaryFileMap = DataInputOutputUtil.readIntegerStringSortedMap(in);
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
        out.writeInt(partTableId);
        out.writeInt(lineitemTableId);
        writeIntArray(out, partPartitionIds);
        writeIntArray(out, lineitemPartitionIds);
        out.writeUTF(brand);
        out.writeUTF(container);
        DataInputOutputUtil.writeIntegerStringSortedMap(out, repartitionSummaryFileMap);
    }
    
    /**
     * Gets the part table id.
     *
     * @return the part table id
     */
    public int getPartTableId() {
        return partTableId;
    }
    
    /**
     * Sets the part table id.
     *
     * @param partTableId the new part table id
     */
    public void setPartTableId(int partTableId) {
        this.partTableId = partTableId;
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
     * Gets the iD of part table's LVReplicaPartition to process in this node.
     *
     * @return the iD of part table's LVReplicaPartition to process in this node
     */
    public int[] getPartPartitionIds() {
        return partPartitionIds;
    }
    
    /**
     * Sets the iD of part table's LVReplicaPartition to process in this node.
     *
     * @param partPartitionIds the new iD of part table's LVReplicaPartition to process in this node
     */
    public void setPartPartitionIds(int[] partPartitionIds) {
        this.partPartitionIds = partPartitionIds;
    }
    
    /**
     * Gets the iD of lineitem table's LVReplicaPartition to process in this node.
     *
     * @return the iD of lineitem table's LVReplicaPartition to process in this node
     */
    public int[] getLineitemPartitionIds() {
        return lineitemPartitionIds;
    }
    
    /**
     * Sets the iD of lineitem table's LVReplicaPartition to process in this node.
     *
     * @param lineitemPartitionIds the new iD of lineitem table's LVReplicaPartition to process in this node
     */
    public void setLineitemPartitionIds(int[] lineitemPartitionIds) {
        this.lineitemPartitionIds = lineitemPartitionIds;
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
