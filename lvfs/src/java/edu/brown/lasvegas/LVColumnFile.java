package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
public class LVColumnFile implements LVObject {
    public static final String IX_PARTITION_ID = "IX_PARTITION_ID";
    /**
     * ID of the sub-partition this column file belongs to.
     */
    @SecondaryKey(name=IX_PARTITION_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVReplicaPartition.class)
    private int partitionId;

    public static final String IX_COLUMN_ID = "IX_COLUMN_ID";
    /**
     * ID of the column this file stores.
     */
    @SecondaryKey(name=IX_COLUMN_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVColumn.class)
    private int columnId;
    
    /**
     * Unique ID of this file.
     */
    @PrimaryKey
    private int columnFileId;
    @Override
    public int getPrimaryKey() {
        return columnFileId;
    }

    /**
     * The file path of this columnar file in data node.
     */
    private String localFilePath;

    /**
     * Byte size of this file. Only used as statistics.
     */
    private int fileSize;
    
    /** CRC32 of the file. */
    private long checksum;
    
    /** The size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0).*/
    private byte dictionaryBytesPerEntry;
    
    /** The number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0). */
    private int distinctValues;
    
    /** The average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).*/
    private int averageRunLength;

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "ColumnFile-" + columnFileId + " (Column=" + columnId + ", Partition=" + partitionId + ")"
            + " localFilePath=" + localFilePath + ", FileSize=" + fileSize + ", checksum=" + checksum
            + ", dictionaryBytesPerEntry=" + dictionaryBytesPerEntry + ", distinctValues=" + distinctValues
            + ", averageRunLength=" + averageRunLength;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(checksum);
        out.writeByte(dictionaryBytesPerEntry);
        out.writeInt(distinctValues);
        out.writeInt(averageRunLength);
        out.writeInt(columnFileId);
        out.writeInt(columnId);
        out.writeInt(fileSize);
        out.writeBoolean(localFilePath == null);
        if (localFilePath != null) {
            out.writeUTF(localFilePath);
        }
        out.writeInt(partitionId);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        checksum = in.readLong();
        dictionaryBytesPerEntry = in.readByte();
        distinctValues = in.readInt();
        averageRunLength = in.readInt();
        columnFileId = in.readInt();
        columnId = in.readInt();
        fileSize = in.readInt();
        if (in.readBoolean()) {
            localFilePath = null;
        } else {
            localFilePath = in.readUTF();
        }
        partitionId = in.readInt();
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVColumnFile read (DataInput in) throws IOException {
        LVColumnFile obj = new LVColumnFile();
        obj.readFields(in);
        return obj;
    }

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.COLUMN_FILE;
    }
    
// auto-generated getters/setters (comments by JAutodoc)
    /**
     * Gets the iD of the sub-partition this column file belongs to.
     *
     * @return the iD of the sub-partition this column file belongs to
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Sets the iD of the sub-partition this column file belongs to.
     *
     * @param partitionId the new iD of the sub-partition this column file belongs to
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
     * Gets the file path of this columnar file in data node.
     *
     * @return the file path of this columnar file in data node
     */
    public String getLocalFilePath() {
        return localFilePath;
    }

    /**
     * Sets the file path of this columnar file in data node.
     *
     * @param localFilePath the new file path of this columnar file in data node
     */
    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    /**
     * Gets the byte size of this file.
     *
     * @return the byte size of this file
     */
    public int getFileSize() {
        return fileSize;
    }

    /**
     * Sets the byte size of this file.
     *
     * @param fileSize the new byte size of this file
     */
    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }
    
    /**
     * Gets the cRC32 of the file.
     *
     * @return the cRC32 of the file
     */
    public long getChecksum() {
        return checksum;
    }
    
    /**
     * Sets the cRC32 of the file.
     *
     * @param checksum the new cRC32 of the file
     */
    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    /**
     * Gets the size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0).
     *
     * @return the size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0)
     */
    public byte getDictionaryBytesPerEntry() {
        return dictionaryBytesPerEntry;
    }

    /**
     * Sets the size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0).
     *
     * @param dictionaryBytesPerEntry the new size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0)
     */
    public void setDictionaryBytesPerEntry(byte dictionaryBytesPerEntry) {
        this.dictionaryBytesPerEntry = dictionaryBytesPerEntry;
    }

    /**
     * Gets the number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0).
     *
     * @return the number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0)
     */
    public int getDistinctValues() {
        return distinctValues;
    }

    /**
     * Sets the number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0).
     *
     * @param distinctValues the new number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0)
     */
    public void setDistinctValues(int distinctValues) {
        this.distinctValues = distinctValues;
    }

    /**
     * Gets the average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).
     *
     * @return the average run length in this file, Set only when the column file is RLE-compressed (otherwise 0)
     */
    public int getAverageRunLength() {
        return averageRunLength;
    }

    /**
     * Sets the average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).
     *
     * @param averageRunLength the new average run length in this file, Set only when the column file is RLE-compressed (otherwise 0)
     */
    public void setAverageRunLength(int averageRunLength) {
        this.averageRunLength = averageRunLength;
    }
    
}
