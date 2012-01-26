package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.LVFSFileType;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedBlockCmpWriter;
import edu.brown.lasvegas.lvfs.TypedDictWriter;
import edu.brown.lasvegas.lvfs.TypedRLEWriter;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalFixLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalValFile;
import edu.brown.lasvegas.lvfs.local.LocalWriterFactory;
import edu.brown.lasvegas.traits.FixLenValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.util.VirtualFileUtil;

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
 * TODO: this assumption should be eliminated in future. Even if there happens a severe skew or a table
 * the user forgets to partition, the performance should gracefully deteriorate, rather than
 * throwing OutOfMemory. The code will become longer, but not that much.
 * </p>
 */
public final class PartitionRewriter {
    private static Logger LOG = Logger.getLogger(PartitionRewriter.class);

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
        assert (newSortColumn == null || (newSortColumn >= 0 && newSortColumn < columnCount)); // if you fail here, didn't you pass column "ID", not the index in the array??

        this.newFiles = new ColumnFileBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            ColumnFileBundle newFile = new ColumnFileBundle();
            newFile.setColumnType(columnTypes[i]);
            newFile.setCompressionType(newCompressions[i]);
            String filename = newFileNames[i]; // note that this filename is WITHOUT file extension.
            newFile.setDataFile(outputFolder.getChildFile(LVFSFileType.DATA_FILE.appendExtension(filename)));
            if (newCompressions[i] == CompressionType.DICTIONARY) {
                newFile.setDictionaryFile(outputFolder.getChildFile(LVFSFileType.DICTIONARY_FILE.appendExtension(filename)));
                assert (newFile.getDictionaryFile() != null);
                if (oldCompressions[i] != CompressionType.DICTIONARY) {
                    newFile.setTmpFile(outputFolder.getChildFile(LVFSFileType.TMP_DATA_FILE.appendExtension(filename)));
                }
            }
            if (newCompressions[i] == CompressionType.RLE
                    || (newCompressions[i] == CompressionType.NONE && (columnTypes[i] == ColumnType.VARBINARY || columnTypes[i] == ColumnType.VARCHAR))) {
                newFile.setPositionFile(outputFolder.getChildFile(LVFSFileType.POSITION_FILE.appendExtension(filename)));
            }
            if (newSortColumn != null && i == newSortColumn) {
                newFile.setSorted(true);
                newFile.setValueFile(outputFolder.getChildFile(LVFSFileType.VALUE_FILE.appendExtension(filename)));
            }
            newFile.setTupleCount(tupleCount);
            newFiles[i] = newFile;
        }
    }

    /**
     * Creates a new set of columnar files based on the existing 'buddy' files.
     * @return created new files.
     * @throws IOException
     */
    public ColumnFileBundle[] execute () throws IOException {
        LOG.info("started");
        if (newSortColumn == null || newSortColumn.equals(oldSortColumn)) {
            LOG.info("no need to re-sort");
            // no sorting, or the same sorting.
            for (int i = 0; i < columnCount; ++i) {
                if (oldCompressions[i] == newCompressions[i]) {
                    // even compression scheme is same. then everything is same, so just copy it.
                    LOG.debug("cool, column file[" + i + "] will be copied as is");
                    inheritEverything(i);
                } else {
                    // if not, then it's re-compression without sorting
                    rewriteNonSortingColumn (i, null); // give null for not re-ordering
                }
            }
        } else {
            // first, read/write sorting column to get order-mapping
            int[] oldPos = rewriteSortingColumn ();
            for (int i = 0; i < columnCount; ++i) {
                if (i == newSortColumn) continue; // we already wrote it in rewriteSortingColumn()
                rewriteNonSortingColumn (i, oldPos);
            }
        }
        LOG.info("done!");
        return newFiles;
    }
    /** on the other hand, if both are dictionary encoding, we can reuse the dictionary file. */
    private boolean willInheritDictionary (int col) {
        return oldCompressions[col] == CompressionType.DICTIONARY && newCompressions[col] == CompressionType.DICTIONARY;
    }
    /** copy the old file's dictionary. */
    private void inheritDictionary (int col) throws IOException {
        ColumnFileBundle oldFile = oldFiles[col];
        ColumnFileBundle newFile = newFiles[col];
        assert (oldFile.getDictionaryFile() != null);
        assert (newFile.getDictionaryFile() != null);
        LOG.info("inheriting dictionary...");
        VirtualFileUtil.copyFile(oldFile.getDictionaryFile(), newFile.getDictionaryFile());
        // also, we can inherit statistics about dictionary
        newFile.setDictionaryBytesPerEntry(oldFile.getDictionaryBytesPerEntry());
        newFile.setDistinctValues(oldFile.getDistinctValues());
        LOG.info("copied dictionary.");
    }
    /** copy a columnar file without changing anything. */
    private void inheritEverything (int col) throws IOException {
        ColumnFileBundle oldFile = oldFiles[col];
        ColumnFileBundle newFile = newFiles[col];
        VirtualFileUtil.copyFile(oldFile.getDataFile(), newFile.getDataFile());
        if (oldFile.getDictionaryFile() != null) {
            VirtualFileUtil.copyFile(oldFile.getDictionaryFile(), newFile.getDictionaryFile());
        }
        if (oldFile.getPositionFile() != null) {
            VirtualFileUtil.copyFile(oldFile.getPositionFile(), newFile.getPositionFile());
        }
        if (oldFile.getValueFile() != null) {
            VirtualFileUtil.copyFile(oldFile.getValueFile(), newFile.getValueFile());
        }
        // also copy all statistics
        newFile.setDataFileChecksum(oldFile.getDataFileChecksum());
        newFile.setDictionaryBytesPerEntry(oldFile.getDictionaryBytesPerEntry());
        newFile.setDistinctValues(oldFile.getDistinctValues());
        newFile.setRunCount(oldFile.getRunCount());
        newFile.setUncompressedSizeKB(oldFile.getUncompressedSizeKB());
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
        LOG.info("re-sorting..");
        // get data traits AFTER compression because it might be dictionary encoded
        ValueTraits dataTraits = oldFilesReader[newSortColumn].getCompressedDataTraits();

        // first, read ALL data of the column to be sorted
        Object keys = readOldData (newSortColumn);
            
        // then, sort them and create a mapping.
        int[] oldPos = new int[tupleCount];
        for (int i = 0; i < tupleCount; ++i) {
            oldPos[i] = i;
        }
        dataTraits.sortKeyValue(keys, oldPos); // coupled-sort on keys and oldPos.

        LOG.debug("reading old file and sorting done!");

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
        
        // write out this column file
        convertAndWriteData (keys, newSortColumn);
        LOG.debug("wrote new data file for the sorting column");
        
        // because this is the sorting column, also write out value index
        List<Object> collectedValues = new ArrayList<Object>();
        List<Integer> collectedPositions = new ArrayList<Integer>();
        for (int i = 0; i < tupleCount; i += 128) { // this 128 should be a runtime parameter
            collectedValues.add(dataTraits.get(keys, i));
            collectedPositions.add(i);
        }
        LocalValFile valueIndex = new LocalValFile(collectedValues, collectedPositions, dataTraits);
        valueIndex.writeToFile(newFiles[newSortColumn].getValueFile());
        
        LOG.debug("value index written");

        // and also get distinct values from it (if it's dictionary encoded, we anyway have the data)
        if (newCompressions[newSortColumn] != CompressionType.DICTIONARY) {
            // because it's sorted, we can efficiently get #distinct-values even without dictionary 
            newFiles[newSortColumn].setDistinctValues(dataTraits.countDistinct(keys));
        }
        
        LOG.info("finished to write the sorting column");
        return oldPos;
    }

    /** Read ALL data from the old columnar file. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object readOldData (int col) throws IOException {
        ValueTraits dataTraits = oldFilesReader[col].getCompressedDataTraits();
        long start = System.currentTimeMillis();
        TypedReader dataReader = oldFilesReader[col].getCompressedDataReader(); // use the reader without dictionary-decompression
        try {
            Object oldData = dataTraits.createArray(tupleCount);
            // read them all! each columnar file should be small.
            int read = dataReader.readValues(oldData, 0, tupleCount);
            assert (read == tupleCount);
            long end = System.currentTimeMillis();
            if (LOG.isDebugEnabled()) {
                LOG.debug("read old data file (" + oldFiles[col].getDataFile().length() + " bytes) in " + (end - start) + "ms");
            }
            return oldData;
        } finally {
            dataReader.close();
        }
    }
    
    /**
     * Re-write a non-sorting column with a different sorting and/or compression.
     * @param oldPos the mapping table to re-order. If this is not null, we
     * re-order the data with this mapping. If null, we don't reorder.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void rewriteNonSortingColumn (int col, int[] oldPos) throws IOException {
        LOG.info("rewriting non-sorting column[" + col + "]..");
        ValueTraits dataTraits = oldFilesReader[col].getCompressedDataTraits();
        Object oldData = readOldData (col);
        Object newData;
        if (oldPos != null) {
            // reorder the data using oldPos. this is quite efficient assuming all objects fit RAM
            newData = dataTraits.reorder(oldData, oldPos);
        } else {
            // keep the order of the data. just apply a different compression.
            newData = oldData;
        }
        oldData = null; // we no longer need it. help GC 
        LOG.info("read and re-ordered old data[" + col + "]. count=" + (dataTraits.length(newData)) + ". writing to the new file...");
        convertAndWriteData (newData, col);
        LOG.info("written new data file.");
    }
    
    /**
     * Writes out the given data to a new file. If needed, this function decompresses
     * dictionary-compressed data.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void convertAndWriteData (Object data, int col) throws IOException {
        ValueTraits dataTraits = oldFilesReader[col].getCompressedDataTraits();
        ValueTraits originalTraits = oldFilesReader[col].getOriginalDataTraits();

        // do we have to make additional conversion because it was dictionary-encoded and we are now changing it?
        if (oldCompressions[col] == CompressionType.DICTIONARY && newCompressions[col] != CompressionType.DICTIONARY) {
            LOG.info("decompression to the original data type...");
            // then, we need to decompress the data before writing them to new file
            Object converted = originalTraits.createArray(tupleCount);
            OrderedDictionary oldDict = oldFilesReader[col].getDictionary();
            if (data instanceof byte[]) {
                oldDict.decompressBatch((byte[]) data, 0, converted, 0, tupleCount);
            } else if (data instanceof short[]) {
                oldDict.decompressBatch((short[]) data, 0, converted, 0, tupleCount);
            } else {
                oldDict.decompressBatch((int[]) data, 0, converted, 0, tupleCount);
            }
            
            // notice we use *original* data traits here.
            writeToNewFile (converted, col, originalTraits);
        } else {
            LOG.info("write as is.");
            // if original compression wasn't dictionary, nothing is tricky.
            writeToNewFile (data, col, dataTraits);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void writeToNewFile (Object data, int col, ValueTraits traits) throws IOException {
        if (willInheritDictionary(col)) {
            inheritDictionary(col);
        }

        long start = System.currentTimeMillis();
        ColumnFileBundle newFile = newFiles[col];
        TypedWriter dataWriter;
        if (willInheritDictionary(col)) {
            dataWriter = new LocalFixLenWriter(newFile.getDataFile(), (FixLenValueTraits<?, ?>) traits);
        } else {
            dataWriter = LocalWriterFactory.getInstance(newFile, newCompressions[col], traits);
        }
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info("writing " + traits.length(data) + " values...");
            }
            dataWriter.writeValues(data, 0, tupleCount);
            long crc32Value = dataWriter.writeFileFooter();
            newFile.setDataFileChecksum(crc32Value);
            if (newFile.getPositionFile() != null) {
                dataWriter.writePositionFile(newFile.getPositionFile());
            }
            dataWriter.flush();
            // collect statistics
            if (dataWriter instanceof TypedDictWriter) {
                newFile.setDistinctValues(((TypedDictWriter) dataWriter).getFinalDict().getDictionarySize());
                newFile.setDictionaryBytesPerEntry(((TypedDictWriter) dataWriter).getFinalDict().getBytesPerEntry());
            } else if (dataWriter instanceof TypedRLEWriter) {
                newFile.setRunCount(((TypedRLEWriter) dataWriter).getRunCount());
            } else if (dataWriter instanceof TypedBlockCmpWriter) {
                long uncompressedSize = ((TypedBlockCmpWriter) dataWriter).getTotalUncompressedSize();
                int uncompressedSizeKB = (int) (uncompressedSize / 1024L + (uncompressedSize % 1024 == 0 ? 0 : 1));
                newFile.setUncompressedSizeKB(uncompressedSizeKB);
            }
            long end = System.currentTimeMillis();
            if (LOG.isInfoEnabled()) {
                LOG.info("wrote new data file (" + newFiles[col].getDataFile().length() + " bytes) in " + (end - start) + "ms");
            }
        } finally {
            dataWriter.close();
        }
    }
}
