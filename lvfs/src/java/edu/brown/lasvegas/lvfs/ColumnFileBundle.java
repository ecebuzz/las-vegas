package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Represents a set of files which logically constitute a column file.
 */
public final class ColumnFileBundle {
    /** empty constructor. */
    public ColumnFileBundle () {
    }
    /**
     * constructs from {@link LVColumnFile} <b>assuming the file is in this data node</b>.
     */
    public ColumnFileBundle (LVColumnFile file) throws IOException {
        this (file, new LocalVirtualFile(file.getLocalFilePath()));
    }
    /**
     * constructs from {@link LVColumnFile} <b>connecting to the remote data node</b>.
     */
    public ColumnFileBundle (LVColumnFile file, LVDataProtocol dataNode) throws IOException {
        this (file, new DataNodeFile(dataNode, file.getLocalFilePath()));
    }
    /** this constructor should expect that the files do NOT exist yet. */
    private ColumnFileBundle (LVColumnFile file, VirtualFile filePath) throws IOException {
        VirtualFile parentFolder = filePath.getParentFile();
        if (!parentFolder.exists()) {
            parentFolder.mkdirs();
            assert (parentFolder.exists());
        }
        this.columnType = file.getColumnType();
        this.compressionType = file.getCompressionType();
        this.dataFileChecksum = file.getChecksum();
        this.dictionaryBytesPerEntry = file.getDictionaryBytesPerEntry();
        this.distinctValues = file.getDistinctValues();
        this.runCount = file.getRunCount();
        this.sorted = file.isSorted();
        this.tupleCount = file.getTupleCount();
        this.uncompressedSizeKB = file.getUncompressedSizeKB();
        
        String filename = filePath.getName(); // note that this filename is WITHOUT file extension.
        this.dataFile = parentFolder.getChildFile(LVFSFileType.DATA_FILE.appendExtension(filename));
        if (compressionType == CompressionType.DICTIONARY) {
            this.dictionaryFile = parentFolder.getChildFile(LVFSFileType.DICTIONARY_FILE.appendExtension(filename));
        }
        if (compressionType == CompressionType.RLE
                || (compressionType == CompressionType.NONE && (columnType == ColumnType.VARBINARY || columnType == ColumnType.VARCHAR))) {
            this.positionFile = parentFolder.getChildFile(LVFSFileType.POSITION_FILE.appendExtension(filename));
        }
        if (isSorted()) {
            this.valueFile = parentFolder.getChildFile(LVFSFileType.VALUE_FILE.appendExtension(filename));
        }
    }
    
    /** main data file. always exists. */
    private VirtualFile dataFile;
    /** CRC32 of the data file. */
    private long dataFileChecksum;
    /** dictionary file. only when dictionary compressed. */
    private VirtualFile dictionaryFile;
    /** position index file. only for some file types. */
    private VirtualFile positionFile;
    /** value index file. only when the file is sorted by the column. */
    private VirtualFile valueFile;
    /** temporary file. only while constructing a new dictionary-encoded file. */
    private VirtualFile tmpFile;
    
    /** Original (before dictionary compression, if any) value type of the column file. */
    private CompressionType compressionType;
    /** Compression type of the column file. */
    private ColumnType columnType;

    /** The size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0).*/
    private byte dictionaryBytesPerEntry;

    /**
     * Whether the column file is sorted by the column.
     * If true, the column file comes with a Value-Index file ({@link ValueIndex}).
     */
    private boolean sorted;
    
    /** The number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0). */
    private int distinctValues;
    
    /** The average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).*/
    private int runCount;

    /** number of tuples in the file. */
    private int tupleCount = 0;

    /** file size without gzip/snappy compression in KB.  Set only when the column file is GZIP/SNAPPY-compressed (otherwise 0). */
    private int uncompressedSizeKB;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FileBundle[ dataFile = " + dataFile + ",dictionaryFile=" + dictionaryFile
        + ",positionFile=" + positionFile + ", valueFile=" + valueFile
        + "]. properties=[compressionType=" + compressionType + ", columnType=" + columnType
        + ", dictionaryBytesPerEntry=" + dictionaryBytesPerEntry + ", sorted=" + sorted
        + ", distinctValues=" + distinctValues + ", runCount=" + runCount
        + ", tupleCount=" + tupleCount + ", uncompressedSizeKB=" + uncompressedSizeKB +"]";
    }
    
    /** delete all files in this bundle. */
    public void deleteFiles () throws IOException {
        tryDeleteFile(dataFile);
        tryDeleteFile(dictionaryFile);
        tryDeleteFile(positionFile);
        tryDeleteFile(valueFile);
        tryDeleteFile(tmpFile);
    }
    private static void tryDeleteFile (VirtualFile file) throws IOException {
        if (file == null) return;
        if (!file.exists()) return;
        if (!file.delete()) {
            throw new IOException ("failed to delete this file:" + file);
        }
    }

// auto-generated getters/setters (comments by JAutodoc)

    /**
     * Gets the main data file.
     *
     * @return the main data file
     */
    public VirtualFile getDataFile() {
        return dataFile;
    }

    /**
     * Sets the main data file.
     *
     * @param dataFile the new main data file
     */
    public void setDataFile(VirtualFile dataFile) {
        this.dataFile = dataFile;
    }

    /**
     * Gets the dictionary file.
     *
     * @return the dictionary file
     */
    public VirtualFile getDictionaryFile() {
        return dictionaryFile;
    }

    /**
     * Sets the dictionary file.
     *
     * @param dictionaryFile the new dictionary file
     */
    public void setDictionaryFile(VirtualFile dictionaryFile) {
        this.dictionaryFile = dictionaryFile;
    }

    /**
     * Gets the position index file.
     *
     * @return the position index file
     */
    public VirtualFile getPositionFile() {
        return positionFile;
    }

    /**
     * Sets the position index file.
     *
     * @param positionFile the new position index file
     */
    public void setPositionFile(VirtualFile positionFile) {
        this.positionFile = positionFile;
    }

    /**
     * Gets the value index file.
     *
     * @return the value index file
     */
    public VirtualFile getValueFile() {
        return valueFile;
    }

    /**
     * Sets the value index file.
     *
     * @param valueFile the new value index file
     */
    public void setValueFile(VirtualFile valueFile) {
        this.valueFile = valueFile;
    }

    /**
     * Gets the original (before dictionary compression, if any) value type of the column file.
     *
     * @return the original (before dictionary compression, if any) value type of the column file
     */
    public CompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Sets the original (before dictionary compression, if any) value type of the column file.
     *
     * @param compressionType the new original (before dictionary compression, if any) value type of the column file
     */
    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    /**
     * Gets the compression type of the column file.
     *
     * @return the compression type of the column file
     */
    public ColumnType getColumnType() {
        return columnType;
    }

    /**
     * Sets the compression type of the column file.
     *
     * @param columnType the new compression type of the column file
     */
    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
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
     * Checks if is whether the column file is sorted by the column.
     *
     * @return the whether the column file is sorted by the column
     */
    public boolean isSorted() {
        return sorted;
    }

    /**
     * Sets the whether the column file is sorted by the column.
     *
     * @param sorted the new whether the column file is sorted by the column
     */
    public void setSorted(boolean sorted) {
        this.sorted = sorted;
    }

    /**
     * Gets the average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).
     *
     * @return the average run length in this file, Set only when the column file is RLE-compressed (otherwise 0)
     */
    public int getRunCount() {
        return runCount;
    }

    /**
     * Sets the average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).
     *
     * @param runCount the new average run length in this file, Set only when the column file is RLE-compressed (otherwise 0)
     */
    public void setRunCount(int runCount) {
        this.runCount = runCount;
    }

    /**
     * Gets the cRC32 of the data file.
     *
     * @return the cRC32 of the data file
     */
    public long getDataFileChecksum() {
        return dataFileChecksum;
    }

    /**
     * Sets the cRC32 of the data file.
     *
     * @param dataFileChecksum the new cRC32 of the data file
     */
    public void setDataFileChecksum(long dataFileChecksum) {
        this.dataFileChecksum = dataFileChecksum;
    }

    /**
     * Gets the number of tuples in the file.
     *
     * @return the number of tuples in the file
     */
    public int getTupleCount() {
        return tupleCount;
    }

    /**
     * Sets the number of tuples in the file.
     *
     * @param tupleCount the new number of tuples in the file
     */
    public void setTupleCount(int tupleCount) {
        this.tupleCount = tupleCount;
    }

    /**
     * Gets the file size without gzip/snappy compression in KB.
     *
     * @return the file size without gzip/snappy compression in KB
     */
    public int getUncompressedSizeKB() {
        return uncompressedSizeKB;
    }

    /**
     * Sets the file size without gzip/snappy compression in KB.
     *
     * @param uncompressedSizeKB the new file size without gzip/snappy compression in KB
     */
    public void setUncompressedSizeKB(int uncompressedSizeKB) {
        this.uncompressedSizeKB = uncompressedSizeKB;
    }
    
    /**
     * Gets the temporary file.
     *
     * @return the temporary file
     */
    public VirtualFile getTmpFile() {
        return tmpFile;
    }
    
    /**
     * Sets the temporary file.
     *
     * @param tmpFile the new temporary file
     */
    public void setTmpFile(VirtualFile tmpFile) {
        this.tmpFile = tmpFile;
    }
    
}
