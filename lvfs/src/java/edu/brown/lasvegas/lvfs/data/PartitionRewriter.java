package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.LVFSFileType;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;

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
    /** how newFiles will be compressed. */
    private final CompressionType[] newCompressions;
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
        this.newCompressions = newCompressions;
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
    
    /** do we have to make additional conversion because it's dictionary to something else or the other way around?. */
    private boolean willInternalDataTypeChange (int col) {
        if (oldCompressions[col] == CompressionType.DICTIONARY && oldCompressions[col] != CompressionType.DICTIONARY) {
            return true;
        }
        if (oldCompressions[col] != CompressionType.DICTIONARY && oldCompressions[col] == CompressionType.DICTIONARY) {
            return true;
        }
        return false;
    }

    public void execute () throws IOException {
        if (newSortColumn == null || newSortColumn.equals(oldSortColumn)) {
            // no sorting, or the same sorting.
            for (int i = 0; i < columnCount; ++i) {
                if (oldCompressions[i] == newCompressions[i]) {
                    // even compression scheme is same. then everything is same, so just copy it.
                    copyIdentical(i);
                } else {
                    // if not, then it's re-compression without sorting
                }
            }
        } else {
            int[] oldPos = rewriteSortingColumn ();
            for (int i = 0; i < columnCount; ++i) {
                if (i == newSortColumn) continue; // we already wrote it in rewriteSortingColumn()
                rewriteNonSortingColumn (i, oldPos);
            }
        }
    }
    /** copy a columnar file without changing anything. */
    private void copyIdentical (int col) throws IOException {
        ColumnFileBundle oldFile = oldFiles[col];
        oldFile.getDataFile();
    }
    
    /**
     * Applies the new sorting on the sorting column and returns a mapping table
     * to convert data in each old file to data in a new file.
     * This method also writes out the new columnar files for the sorting column
     * because we already read the old file.
     * @return oldPos: a mapping table to re-order data. For example, if oldPos[3]=20,
     * the tuple 3 in a new file should take the value from tuple 20 in the old file.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int[] rewriteSortingColumn () throws IOException {
        assert (newSortColumn != null);

        // first, read ALL data of the column to be sorted
        TypedReader dataReader = oldFilesReader[newSortColumn].getDataReader();
        try {
            // get data traits AFTER compression because it might be dictionary encoded
            ValueTraits dataTraits = oldFilesReader[newSortColumn].getCompressedDataTraits();
            Object keys = dataTraits.createArray(tupleCount);
            // read them all! the sorting column file should be small. (can't imagine sorting by a big column)
            int read = dataReader.readValues(keys, 0, tupleCount);
            assert (read == tupleCount);
            
            // then, sort them and create a mapping.
            int[] oldPos = new int[tupleCount];
            for (int i = 0; i < tupleCount; ++i) {
                oldPos[i] = i;
            }
            dataTraits.sortKeyValue(keys, oldPos); // coupled-sort on keys and oldPos.
    
            // similar comments in LocalDictCompressionWriter#writeFileFooter(). what happens there is somewhat analogous to this
            
            // for example, suppose the column data in old file was: D,A,C,B,E
            // before sorting, oldPos[] was: 0,1,2,3,4. After sorting: 1,3,2,0,4
            // thus, each column file should be re-ordered as follows
            //       old pos=1 (key=A): new pos=0
            //       old pos=3 (key=B): new pos=1
            //       old pos=2 (key=C): new pos=2
            //       old pos=0 (key=D): new pos=3
            //       old pos=4 (key=E): new pos=4
            // we use oldPos[] to do this in subsequent functions
            
            // TODO write out sorting column file
            if (willInternalDataTypeChange(newSortColumn)) {
                
            } else {
                
            }
            
            
            return oldPos;
        } finally {
            dataReader.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void rewriteNonSortingColumn (int col, int[] oldPos) throws IOException {
        TypedReader dataReader = oldFilesReader[col].getDataReader();
        try {
            ValueTraits dataTraits = oldFilesReader[col].getCompressedDataTraits();
            Object oldData = dataTraits.createArray(tupleCount);
            int read = dataReader.readValues(oldData, 0, tupleCount);
            assert (read == tupleCount);
            
            Object newData = dataTraits.reorder(oldData, oldPos);
            if (willInternalDataTypeChange(col)) {
                
            } else {
                
            }
        } finally {
            dataReader.close();
        }
    }
}
