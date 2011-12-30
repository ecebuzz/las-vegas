package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.LVFSFileType;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Re-writes all columnar files in a partition with new sorting and compression scheme.
 * <p>This class is used by a few tasks, data import, recovery, and physical design change.</p>
 * 
 * <p>
 * This class assumes there are 'buddy' columnar files to be based on in the same partition,
 * the same fracture, and the same replica group. Thanks to the original buddy files, this class
 * efficiently constructs the new set of files, for example inheriting dictionary files and merely
 * re-sorting compressed integer files. 
 * </p>
 * 
 * <p>
 * This class also assumes that each columnar file is enough small to (though for short time) fully load onto RAM.
 * Because we have several levels of partitioning (fracture, partition, columns) and compression,
 * this should be a reasonable assumption. Each columnar file should be at most 10MB or so.
 * </p>
 */
public final class PartitionRewriter {
    /** number of columns to deal with (maybe not all columns in the partition, but must contain the new sorting column). */
    private final int columnCount;
    /** number of tuples in the files. we know this value because we already have the buddy files in the same partition. */
    private final int tupleCount;
    /** column types BEFORE compression. */
    private final ColumnType[] columnTypes;

    /** existing buddy columnar files. */
    private final ColumnFileBundle[] oldFiles;
    /** reader object for existing buddy columnar files. */
    private final ColumnFileReaderBundle[] oldFilesReader;
    /** newly created files. */
    private final ColumnFileBundle[] newFiles;
    /** how oldFiles were compressed. */
    private final CompressionType[] oldCompressions;
    /** the sorting column. index in the array (0 to columnCount-1). null if no sorting. */
    private final Integer oldSortColumn;
    /** the sorting column. index in the array (0 to columnCount-1). null if no sorting. */
    private final Integer newSortColumn;

    /**
     * Instantiates the re-writer object.
     * @param outputFolder the folder to output newly created files
     * @param oldFiles existing buddy columnar files
     * @param newFileNames filename seed of the new columnar files
     * @param newCompressions how new files will be compressed 
     * @param newSortColumn the sorting column. index in the array (0 to columnCount-1). null if no sorting.
     */
    public PartitionRewriter (VirtualFile outputFolder,
                    ColumnFileBundle[] oldFiles, String[] newFileNames,
                    CompressionType[] newCompressions, Integer newSortColumn) {
        this.oldFiles = oldFiles;
        this.columnCount = oldFiles.length;
        assert (columnCount == newFileNames.length);
        this.tupleCount = oldFiles[0].getTupleCount();
        this.columnTypes = new ColumnType[columnCount];
        this.oldCompressions = new CompressionType[columnCount];
        assert (columnCount == newCompressions.length);
        int oldSortColumnInt = -1;
        this.oldFilesReader = new ColumnFileReaderBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            columnTypes[i] = oldFiles[i].getColumnType();
            oldCompressions[i] = oldFiles[i].getCompressionType();
            oldFilesReader[i] = new ColumnFileReaderBundle(oldFiles[i]);
            if (oldFiles[i].isSorted()) {
                assert (oldSortColumnInt == -1); // no two sorting columns
                oldSortColumnInt = i;
            }
        }
        this.oldSortColumn = (oldSortColumnInt == -1 ? null : oldSortColumnInt);
        this.newSortColumn = newSortColumn;
        assert (newSortColumn == null || (newSortColumn >= 0 && newSortColumn < columnCount));

        this.newFiles = new ColumnFileBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            ColumnFileBundle newFile = new ColumnFileBundle();
            newFile.setColumnType(columnTypes[i]);
            newFile.setCompressionType(newCompressions[i]);
            String filename = newFileNames[i]; // note that this filename is WITHOUT file extension.
            newFile.setDataFile(outputFolder.getChildFile(LVFSFileType.DATA_FILE.appendExtension(filename)));
            if (newCompressions[i] == CompressionType.DICTIONARY) {
                newFile.setDictionaryFile(outputFolder.getChildFile(LVFSFileType.DICTIONARY_FILE.appendExtension(filename)));
            }
            if (newCompressions[i] == CompressionType.RLE
                    || (newCompressions[i] == CompressionType.NONE && (columnTypes[i] == ColumnType.VARBINARY || columnTypes[i] == ColumnType.VARCHAR))) {
                newFile.setPositionFile(outputFolder.getChildFile(LVFSFileType.POSITION_FILE.appendExtension(filename)));
            }
            if (newSortColumn != null && i == newSortColumn) {
                newFile.setSorted(true);
                newFile.setValueFile(outputFolder.getChildFile(LVFSFileType.VALUE_FILE.appendExtension(filename)));
            }
            newFiles[i] = newFile;
        }
    }
    
    public void execute () throws IOException {
        if (newSortColumn == null || newSortColumn.equals(oldSortColumn)) {
            for (int i = 0; i < columnCount; ++i) {
                copyIdentical(i);
            }
        } else {
            
        }
    }
    /** copy a columnar file without changing anything. */
    private void copyIdentical (int col) throws IOException {
        ColumnFileBundle oldFile = oldFiles[col];
        oldFile.getDataFile();
    }
    
    private void sortKeyColumn () throws IOException {
        assert (newSortColumn != null);
        // ValueTraits<?, ?> compressedValue
    }
}
