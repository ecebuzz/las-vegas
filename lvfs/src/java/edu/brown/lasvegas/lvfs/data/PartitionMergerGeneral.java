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
import edu.brown.lasvegas.lvfs.TypedBlockCmpWriter;
import edu.brown.lasvegas.lvfs.TypedDictWriter;
import edu.brown.lasvegas.lvfs.TypedRLEWriter;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalValFile;
import edu.brown.lasvegas.lvfs.local.LocalWriterFactory;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * Partition merging that does not assume the uniform replica scheme.
 * Unlike {@link PartitionMergerForSameScheme}, this needs re-sorting of the merged files.
 * Used while repartitioning and foreign recovery.
 * This is definitely more complex, but instead this class doesn't pay too much effort
 * to reduce CPU overhead. repartitioning/foreign-recovery is anyway very slow..
 */
public final class PartitionMergerGeneral {
    private static Logger LOG = Logger.getLogger(PartitionMergerGeneral.class);

    /** number of columns. */
    private final int columnCount;
    
    /** number of existing partitions to be based on. */
    private final int basePartitionCount;
    /** number of tuples after merging. */
    private final int tupleCount;

    private final ColumnType[] columnTypes;
    /** traits for the column types BEFORE compression*/
    @SuppressWarnings("rawtypes")
    private final ValueTraits[] traits;

    /** existing columnar files. [0 to basePartitions.len-1][0 to columnCount-1]. */
    private final ColumnFileBundle[][] baseFiles;

    /** the sorting column. index in the array (0 to columnCount-1). null if no sorting. */
    private final Integer sortColumn;
    

    /**
     * Instantiates the merger object.
     * @param baseFiles existing columnar files [0 to basePartitions.len-1][0 to columnCount-1]
     * @param columnTypes column types BEFORE compression 
     * @param sortColumn the sorting column. index in the array (0 to columnCount-1). null if no sorting.
     */
    public PartitionMergerGeneral (ColumnFileBundle[][] baseFiles, ColumnType[] columnTypes, Integer sortColumn) {
        this.baseFiles = baseFiles;
        this.basePartitionCount = baseFiles.length;
        this.columnCount = columnTypes.length;
        this.columnTypes = columnTypes;


        this.sortColumn = sortColumn;
        assert (sortColumn == null || (sortColumn >= 0 && sortColumn < columnCount)); // if you fail here, didn't you pass column "ID", not the index in the array??
        
        long tupleCountSum = 0; // number of tuples after merging
        for (int i = 0; i < basePartitionCount; ++i) {
            assert (columnCount == baseFiles[i].length);
            for (int j = 0; j < columnCount; ++j) {
                assert (columnTypes[j] == baseFiles[i][j].getColumnType());
            }
            tupleCountSum += baseFiles[i][0].getTupleCount();
        }
        assert (tupleCountSum < (1L << 31));
        this.tupleCount = (int) tupleCountSum;

        this.traits = new ValueTraits[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            traits[i] = ValueTraitsFactory.getInstance(columnTypes[i]);
        }
    }

    /**
     * Merge columnar files and sort it.
     * Return the data object without saving to files.
     * If you are going to use the merged data just for querying, this is enough.
     * @return All column data merged together (each Object is an array).
     * @throws IOException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public Object[] executeOnMemory () throws IOException {
	    Object[] mergedData = new Object[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            mergedData[i] = traits[i].createArray((int) tupleCount);
        }

        LOG.info("started");
        int copiedTupleCount = 0;
        for (int i = 0; i < basePartitionCount; ++i) {
            int subTotalTupleCount = baseFiles[i][0].getTupleCount();
            for (int j = 0; j < columnCount; ++j) {
            	ColumnFileReaderBundle reader = new ColumnFileReaderBundle(baseFiles[i][j], 1 << 20);
            	try {
	            	assert (subTotalTupleCount == baseFiles[i][j].getTupleCount());
	            	int read = ((TypedReader) reader.getDataReader()).readValues(mergedData[j], copiedTupleCount, subTotalTupleCount);
	            	assert (read == subTotalTupleCount);
            	} finally {
	            	reader.close();
            	}
            }
        }
        LOG.info("merged data");

        if (sortColumn != null) {
            LOG.info("sorting merged data...");
            long start = System.currentTimeMillis();
            sortMergedData(mergedData);
            long end = System.currentTimeMillis();
            LOG.info("sorting done! " + (end - start) + "ms");
        }

        return mergedData;
	}
    /**
     * Same algorithm as {@link PartitionRewriter#rewriteSortingColumn()}.
     */
    @SuppressWarnings("unchecked")
	private void sortMergedData (Object[] mergedData) {
        LOG.debug("organizing the sorting column..");
        long start = System.currentTimeMillis();
        int[] oldPos = new int[tupleCount];
        for (int i = 0; i < tupleCount; ++i) {
            oldPos[i] = i;
        }
        traits[sortColumn].sortKeyValue(mergedData[sortColumn], oldPos);
        long end = System.currentTimeMillis();
        LOG.info("organized the sorting column! " + (end - start) + "ms");

        for (int i = 0; i < columnCount; ++i) {
        	if (i == sortColumn) {
        		continue;
        	}
        	traits[i].reorder(mergedData[i], oldPos);
        }
        long end2 = System.currentTimeMillis();
        LOG.info("organized other columns! " + (end2 - end) + "ms");
    }
    
    /**
     * Merge columnar files, sort them, and then store them to files.
     * @param outputFolder the folder to output newly created files
     * @param newFileNames filename seed of the new columnar files
     * @param compressions how all of new and existing columnar files are compressed 
     * @return created new files.
     * @throws IOException
     */
	public ColumnFileBundle[] executeOnDisk (VirtualFile outputFolder, String[] newFileNames, CompressionType[] compressions) throws IOException {
        assert (columnCount == compressions.length);
        // newly created files after merging.
        ColumnFileBundle[] newFiles = new ColumnFileBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            ColumnFileBundle newFile = new ColumnFileBundle();
            newFile.setColumnType(columnTypes[i]);
            newFile.setCompressionType(compressions[i]);
            String filename = newFileNames[i]; // note that this filename is WITHOUT file extension.
            newFile.setDataFile(outputFolder.getChildFile(LVFSFileType.DATA_FILE.appendExtension(filename)));
            if (compressions[i] == CompressionType.DICTIONARY) {
                newFile.setDictionaryFile(outputFolder.getChildFile(LVFSFileType.DICTIONARY_FILE.appendExtension(filename)));
                assert (newFile.getDictionaryFile() != null);
            }
            if (compressions[i] == CompressionType.RLE
                    || (compressions[i] == CompressionType.NONE && (columnTypes[i] == ColumnType.VARBINARY || columnTypes[i] == ColumnType.VARCHAR))) {
                newFile.setPositionFile(outputFolder.getChildFile(LVFSFileType.POSITION_FILE.appendExtension(filename)));
            }
            if (sortColumn != null && i == sortColumn) {
                newFile.setSorted(true);
                newFile.setValueFile(outputFolder.getChildFile(LVFSFileType.VALUE_FILE.appendExtension(filename)));
            }
            newFile.setTupleCount((int) tupleCount);
            newFiles[i] = newFile;
        }

        Object[] mergedData = executeOnMemory();
        for (int i = 0; i < columnCount; ++i) {
        	writeMergedData (i, mergedData[i], newFiles[i], compressions[i]);
        	mergedData[i] = null;
        }
        LOG.info("done");
        return newFiles;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeMergedData (int col, Object mergedData, ColumnFileBundle newFile, CompressionType compression) throws IOException {
        TypedWriter dataWriter = LocalWriterFactory.getInstance(newFile, compression, traits[col]);
        try {
            long start = System.currentTimeMillis();
            dataWriter.writeValues(mergedData, 0, tupleCount);
            long end = System.currentTimeMillis();
            LOG.info("wrote merged data file (" + tupleCount + " tuples. column=" + col + ") in " + (end - start) + "ms");
            finishDataWriter (newFile, dataWriter);
        } finally {
            dataWriter.close();
        }
        
        if (sortColumn != null && sortColumn == col) {
            // because this is the sorting column, also write out value index
            List<Object> collectedValues = new ArrayList<Object>();
            List<Integer> collectedPositions = new ArrayList<Integer>();
            for (int i = 0; i < tupleCount; i += 128) { // this 128 should be a runtime parameter
                collectedValues.add(traits[col].get(mergedData, i));
                collectedPositions.add(i);
            }
            LocalValFile valueIndex = new LocalValFile(collectedValues, collectedPositions, traits[col]);
            valueIndex.writeToFile(newFile.getValueFile());
            LOG.debug("value index written");

            // and also get distinct values from it (if it's dictionary encoded, we anyway have the data)
            if (compression != CompressionType.DICTIONARY) {
                // because it's sorted, we can efficiently get #distinct-values even without dictionary 
                newFile.setDistinctValues(traits[col].countDistinct(mergedData));
            }
            
            LOG.info("finished to write the sorting column");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private static void finishDataWriter (ColumnFileBundle newFile, TypedWriter dataWriter) throws IOException {
        long crc32Value = dataWriter.writeFileFooter();
        newFile.setDataFileChecksum(crc32Value);
        if (newFile.getPositionFile() != null) {
            dataWriter.writePositionFile(newFile.getPositionFile());
        }
        dataWriter.flush();
        // collect statistics
        assert (!(dataWriter instanceof TypedDictWriter));
        if (dataWriter instanceof TypedDictWriter) {
        	TypedDictWriter dictWriter = (TypedDictWriter) dataWriter;
        	newFile.setDistinctValues(dictWriter.getFinalDict().getDictionarySize());
        	newFile.setDictionaryBytesPerEntry(dictWriter.getFinalDict().getBytesPerEntry());
        } else if (dataWriter instanceof TypedRLEWriter) {
            newFile.setRunCount(((TypedRLEWriter) dataWriter).getRunCount());
        } else if (dataWriter instanceof TypedBlockCmpWriter) {
            long uncompressedSize = ((TypedBlockCmpWriter) dataWriter).getTotalUncompressedSize();
            int uncompressedSizeKB = (int) (uncompressedSize / 1024L + (uncompressedSize % 1024 == 0 ? 0 : 1));
            newFile.setUncompressedSizeKB(uncompressedSizeKB);
        }
    }
}
