package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.CompressionTypeArraySerializer;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;
import edu.brown.lasvegas.util.DataInputOutputUtil;
import edu.brown.lasvegas.util.ValueRange;
import edu.brown.lasvegas.util.ValueRangeArraySerializer;

/**
 * Parameters for {@link RepartitionTaskRunner}.
 */
public final class RepartitionTaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new repartition task parameters.
     */
    public RepartitionTaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new repartition task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public RepartitionTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new repartition task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public RepartitionTaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /**
     * ID of LVColumn that will be used as partitioning column.
     */
    private int partitioningColumnId;
    
    /**
     * The key ranges of the partitioning column.
     * Sorted by the ranges themselves.
     */
    private ValueRange[] partitionRanges;

    /**
     * ID of LVColumn to output.
     */
    private int[] outputColumnIds;
    
    /**
     * Compression types of each outputColumnIds entry.
     */
    private CompressionType[] outputCompressions;
    
    /**
     * ID of LVReplicaPartition to repartition. These must be in this node.
     */
    private int[] basePartitionIds;
    
    /**
     * the maximum number of fragments to write out at each level. if num of partitions is larger than this, we recursively repartition
     * to limit the number of open files and memory consumption.
     */ 
    private int maxFragments = 1 << 7;

    /**
     * The number of tuples to read at once.
     */
    private int readCacheTuples = 1 << 16;

    /** the byte size of buffer for _all_ column file writers at each level. */
    private long writeBufferSizeTotal = 1 << 22;
    
    /**
     * Write.
     *
     * @param out the out
     * @throws IOException Signals that an I/O exception has occurred.
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(partitioningColumnId);
        new ValueRangeArraySerializer().writeArray(out, partitionRanges);
        DataInputOutputUtil.writeIntArray(out, outputColumnIds);
        new CompressionTypeArraySerializer().writeArray(out, outputCompressions);
        DataInputOutputUtil.writeIntArray(out, basePartitionIds);
        out.writeInt(maxFragments);
        out.writeInt(readCacheTuples);
        out.writeLong(writeBufferSizeTotal);
    }

    /**
     * Read fields.
     *
     * @param in the in
     * @throws IOException Signals that an I/O exception has occurred.
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
    	partitioningColumnId = in.readInt();
    	partitionRanges = new ValueRangeArraySerializer().readArray(in);
    	outputColumnIds = DataInputOutputUtil.readIntArray(in);
    	outputCompressions = new CompressionTypeArraySerializer().readArray(in);
    	basePartitionIds = DataInputOutputUtil.readIntArray(in);
    	maxFragments = in.readInt();
    	readCacheTuples = in.readInt();
    	writeBufferSizeTotal = in.readLong();
    }

    // auto-generated getters/setters (comments by JAutodoc)    
	/**
     * Gets the partitioning column id.
     *
     * @return the partitioning column id
     */
    public int getPartitioningColumnId() {
		return partitioningColumnId;
	}

	/**
	 * Sets the partitioning column id.
	 *
	 * @param partitioningColumnId the new partitioning column id
	 */
	public void setPartitioningColumnId(int partitioningColumnId) {
		this.partitioningColumnId = partitioningColumnId;
	}

	/**
	 * Gets the partition ranges.
	 *
	 * @return the partition ranges
	 */
	public ValueRange[] getPartitionRanges() {
		return partitionRanges;
	}

	/**
	 * Sets the partition ranges.
	 *
	 * @param partitionRanges the new partition ranges
	 */
	public void setPartitionRanges(ValueRange[] partitionRanges) {
		this.partitionRanges = partitionRanges;
	}

	/**
	 * Gets the output column ids.
	 *
	 * @return the output column ids
	 */
	public int[] getOutputColumnIds() {
		return outputColumnIds;
	}

	/**
	 * Sets the output column ids.
	 *
	 * @param outputColumnIds the new output column ids
	 */
	public void setOutputColumnIds(int[] outputColumnIds) {
		this.outputColumnIds = outputColumnIds;
	}

	/**
	 * Gets the output compressions.
	 *
	 * @return the output compressions
	 */
	public CompressionType[] getOutputCompressions() {
		return outputCompressions;
	}

	/**
	 * Sets the output compressions.
	 *
	 * @param outputCompressions the new output compressions
	 */
	public void setOutputCompressions(CompressionType[] outputCompressions) {
		this.outputCompressions = outputCompressions;
	}

	/**
	 * Gets the base partition ids.
	 *
	 * @return the base partition ids
	 */
	public int[] getBasePartitionIds() {
		return basePartitionIds;
	}

	/**
	 * Sets the base partition ids.
	 *
	 * @param basePartitionIds the new base partition ids
	 */
	public void setBasePartitionIds(int[] basePartitionIds) {
		this.basePartitionIds = basePartitionIds;
	}

    /**
     * Gets the maximum number of fragments to write out at each level.
     *
     * @return the maximum number of fragments to write out at each level
     */
    public int getMaxFragments() {
        return maxFragments;
    }

    /**
     * Sets the maximum number of fragments to write out at each level.
     *
     * @param maxFragments the new maximum number of fragments to write out at each level
     */
    public void setMaxFragments(int maxFragments) {
        this.maxFragments = maxFragments;
    }

    /**
     * Gets the number of tuples to read at once.
     *
     * @return the number of tuples to read at once
     */
    public int getReadCacheTuples() {
        return readCacheTuples;
    }

    /**
     * Sets the number of tuples to read at once.
     *
     * @param readCacheTuples the new number of tuples to read at once
     */
    public void setReadCacheTuples(int readCacheTuples) {
        this.readCacheTuples = readCacheTuples;
    }

    /**
     * Gets the byte size of buffer for _all_ column file writers at each level.
     *
     * @return the byte size of buffer for _all_ column file writers at each level
     */
    public long getWriteBufferSizeTotal() {
        return writeBufferSizeTotal;
    }

    /**
     * Sets the byte size of buffer for _all_ column file writers at each level.
     *
     * @param writeBufferSizeTotal the new byte size of buffer for _all_ column file writers at each level
     */
    public void setWriteBufferSizeTotal(long writeBufferSizeTotal) {
        this.writeBufferSizeTotal = writeBufferSizeTotal;
    }
}
