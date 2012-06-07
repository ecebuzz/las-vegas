package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;

import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;
import edu.brown.lasvegas.util.DataInputOutputUtil;

/**
 * Parameters for {@link RecoverPartitionFromRepartitionedFilesTaskRunner}.
 */
public class RecoverPartitionFromRepartitionedFilesTaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new recover partition from repartitioned files task parameters.
     */
    public RecoverPartitionFromRepartitionedFilesTaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new recover partition from repartitioned files task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public RecoverPartitionFromRepartitionedFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new recover partition from repartitioned files task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public RecoverPartitionFromRepartitionedFilesTaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /**
     * ID of the replica ({@link LVReplica}) to be recovered at this data node.
     */
    private int replicaId;
    
    /**
     * ID of the partitions ({@link LVReplicaPartition}) to be recovered at this data node.
     */
    private int[] partitionIds;

    /**
     * location of summary files of the repartitioned files.
     */
    private SortedMap<Integer, String> repartitionSummaryFileMap;

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        replicaId = in.readInt();
        partitionIds = DataInputOutputUtil.readIntArray(in);
        repartitionSummaryFileMap = DataInputOutputUtil.readIntegerStringSortedMap(in);
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(replicaId);
        DataInputOutputUtil.writeIntArray(out, partitionIds);
        DataInputOutputUtil.writeIntegerStringSortedMap(out, repartitionSummaryFileMap);
    }
    
    /**
     * Gets the iD of the replica ({@link LVReplica}) to be recovered at this data node.
     *
     * @return the iD of the replica ({@link LVReplica}) to be recovered at this data node
     */
    public int getReplicaId() {
        return replicaId;
    }
    
    /**
     * Sets the iD of the replica ({@link LVReplica}) to be recovered at this data node.
     *
     * @param replicaId the new iD of the replica ({@link LVReplica}) to be recovered at this data node
     */
    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }
    
    /**
     * Gets the iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node.
     *
     * @return the iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node
     */
    public int[] getPartitionIds() {
        return partitionIds;
    }
    
    /**
     * Sets the iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node.
     *
     * @param partitionIds the new iD of the partitions ({@link LVReplicaPartition}) to be recovered at this data node
     */
    public void setPartitionIds(int[] partitionIds) {
        this.partitionIds = partitionIds;
    }
    
    /**
     * Gets the location of summary files of the repartitioned files.
     *
     * @return the location of summary files of the repartitioned files
     */
    public SortedMap<Integer, String> getRepartitionSummaryFileMap() {
        return repartitionSummaryFileMap;
    }
    
    /**
     * Sets the location of summary files of the repartitioned files.
     *
     * @param repartitionSummaryFileMap the new location of summary files of the repartitioned files
     */
    public void setRepartitionSummaryFileMap(SortedMap<Integer, String> repartitionSummaryFileMap) {
        this.repartitionSummaryFileMap = repartitionSummaryFileMap;
    }
}
