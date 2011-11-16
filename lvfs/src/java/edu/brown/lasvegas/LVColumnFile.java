package edu.brown.lasvegas;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * A columnar file in a replica partition.
 * The smallest unit of data file objects.
 * Each column file belongs to one {@link LVReplicaPartition}.
 * All column files that belong to the same replica partition
 * are located in the same node.
 */
@Entity
public class LVColumnFile {
    /**
     * ID of the replica partition this column file belongs to.
     */
    @SecondaryKey(name="IX_PARTITION_ID", relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaPartition.class)
    private int partitionId;

    /**
     * ID of the column this file stores.
     */
    private int columnId;

    /**
     * Unique ID of this file.
     */
    @PrimaryKey
    private int columnFileId;
    
    /**
     * The file path of this columnar file in HDFS.
     */
    private String hdfsFilePath;

    /**
     * Byte size of this file. Only used as statistics.
     */
    private long fileSize;
    
    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "ColumnFile-" + columnFileId + " (Column=" + columnId + ", Partition=" + partitionId + ")"
            + " HDFS-PATH=" + hdfsFilePath + ", FileSize=" + fileSize;
    }
    
// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the replica partition this column file belongs to.
     *
     * @return the iD of the replica partition this column file belongs to
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Sets the iD of the replica partition this column file belongs to.
     *
     * @param partitionId the new iD of the replica partition this column file belongs to
     */
    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * Gets the iD of the column this file stores.
     *
     * @return the iD of the column this file stores
     */
    public int getColumnId() {
        return columnId;
    }

    /**
     * Sets the iD of the column this file stores.
     *
     * @param columnId the new iD of the column this file stores
     */
    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    /**
     * Gets the unique ID of this file.
     *
     * @return the unique ID of this file
     */
    public int getColumnFileId() {
        return columnFileId;
    }

    /**
     * Sets the unique ID of this file.
     *
     * @param columnFileId the new unique ID of this file
     */
    public void setColumnFileId(int columnFileId) {
        this.columnFileId = columnFileId;
    }

    /**
     * Gets the file path of this columnar file in HDFS.
     *
     * @return the file path of this columnar file in HDFS
     */
    public String getHdfsFilePath() {
        return hdfsFilePath;
    }

    /**
     * Sets the file path of this columnar file in HDFS.
     *
     * @param hdfsFilePath the new file path of this columnar file in HDFS
     */
    public void setHdfsFilePath(String hdfsFilePath) {
        this.hdfsFilePath = hdfsFilePath;
    }

    /**
     * Gets the byte size of this file.
     *
     * @return the byte size of this file
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * Sets the byte size of this file.
     *
     * @param fileSize the new byte size of this file
     */
    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }
    
}
