package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

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
import edu.brown.lasvegas.lvfs.data.task.MergePartitionSameSchemeTaskRunner;
import edu.brown.lasvegas.lvfs.local.LocalDictFile;
import edu.brown.lasvegas.lvfs.local.LocalFixLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalWriterFactory;
import edu.brown.lasvegas.traits.FixLenValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * Core implementation of partition merging ({@link MergePartitionSameSchemeTaskRunner}).
 * Separated for better modularization and thus ease of testing.
 * Again, this one assumes the uniform replica scheme. Many things are simplified because of
 * that, at the cost of possibly additional network I/O.
 */
public final class PartitionMergerForSameScheme {
    private static Logger LOG = Logger.getLogger(PartitionMergerForSameScheme.class);

    /** number of columns. */
    private final int columnCount;
    
    /** number of existing partitions to be based on. */
    private final int basePartitionCount;

    /** original column types BEFORE compression. */
    private final ColumnType[] columnTypes;

    /** traits for the column types BEFORe compression*/
    @SuppressWarnings("rawtypes")
    private final ValueTraits[] originalTraits;


    /** existing columnar files. [0 to basePartitions.len-1][0 to columnCount-1]. */
    private final ColumnFileBundle[][] baseFiles;
    /** reader object for existing columnar files. [0 to basePartitions.len-1][0 to columnCount-1]. */
    private final ColumnFileReaderBundle[][] baseFilesReader;
    /** newly created files after merging. */
    private final ColumnFileBundle[] newFiles;
    /** how all of new and existing columnar files are compressed. */
    private final CompressionType[] compressions;

    /** the sorting column. index in the array (0 to columnCount-1). null if no sorting. */
    private final Integer sortColumn;
    
    /**
     * mapping from values in each base partition to values in new partition.
     * [0 to columnCount-1][0 to basePartitions.len-1][index in old dictionary]
     */
    private int[][][] dictionaryConversion;

    /**
     * Size of dictionary after merging. A little bit tricky if this is different from
     * the bytePerEntry before merging. 
     */
    private byte[] newDictionaryBytesPerEntry;

    /**
     * Instantiates the merger object.
     * @param outputFolder the folder to output newly created files
     * @param baseFiles existing columnar files [0 to basePartitions.len-1][0 to columnCount-1]
     * @param newFileNames filename seed of the new columnar files
     * @param columnTypes column types BEFORE compression 
     * @param compressions how all of new and existing columnar files are compressed 
     * @param sortColumn the sorting column. index in the array (0 to columnCount-1). null if no sorting.
     */
    public PartitionMergerForSameScheme (VirtualFile outputFolder,
                    ColumnFileBundle[][] baseFiles, String[] newFileNames,
                    ColumnType[] columnTypes, CompressionType[] compressions, Integer sortColumn) {
        this.baseFiles = baseFiles;
        this.basePartitionCount = baseFiles.length;
        this.columnCount = newFileNames.length;

        this.columnTypes = columnTypes;
        assert (columnCount == columnTypes.length);

        this.compressions = compressions;
        assert (columnCount == compressions.length);

        this.sortColumn = sortColumn;
        assert (sortColumn == null || (sortColumn >= 0 && sortColumn < columnCount)); // if you fail here, didn't you pass column "ID", not the index in the array??

        this.baseFilesReader = new ColumnFileReaderBundle[baseFiles.length][columnCount];
        long tupleCount = 0; // number of tuples after merging
        for (int i = 0; i < basePartitionCount; ++i) {
            assert (columnCount == baseFiles[i].length);
            for (int j = 0; j < columnCount; ++j) {
                assert (columnTypes[i] == baseFiles[i][j].getColumnType());
                assert (compressions[i] == baseFiles[i][j].getCompressionType());
                baseFilesReader[i][j] = new ColumnFileReaderBundle(baseFiles[i][j], 1 << 20);
            }
            tupleCount += baseFiles[i][0].getTupleCount();
        }
        assert (tupleCount < (1L << 31));

        this.originalTraits = new ValueTraits[columnCount];
        this.newFiles = new ColumnFileBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            originalTraits[i] = ValueTraitsFactory.getInstance(columnTypes[i]);
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
    }

    /**
     * Creates a new set of columnar files by merging the existing files.
     * @return created new files.
     * @throws IOException
     */
    public ColumnFileBundle[] execute () throws IOException {
        LOG.info("started");
        prepareDictionary ();
        
        if (sortColumn == null) {
            // easy. just append one by one
            for (int i = 0; i < columnCount; ++i) {
                copyBaseDataNoSorting(i);
            }
        } else {
            // need to merge from each file according to the sorting column
            mergeSortedBaseData();
        }
        LOG.info("done");
        return newFiles;
    }
    
    /**
     * Creates new dictionary files and also prepares the mapping from old values to new values. 
     * @throws IOException
     */
    private void prepareDictionary () throws IOException {
        LOG.info("preparing dictionary...");
        this.dictionaryConversion = new int[columnCount][basePartitionCount][];
        this.newDictionaryBytesPerEntry = new byte[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            if (compressions[i] != CompressionType.DICTIONARY) {
                continue;
            }
            assert (newFiles[i].getDictionaryFile() != null);
            
            // first, load all dictionary
            LOG.info("loading all dictionaries for column-" + i + "...");
            OrderedDictionary<?, ?>[] baseDicts = new OrderedDictionary<?, ?>[basePartitionCount];
            Object[] baseDictsEntries = new Object[basePartitionCount];
            for (int j = 0; j < basePartitionCount; ++j) {
                baseDicts[j] = baseFilesReader[j][i].getDictionary();
                baseDictsEntries[j] = baseDicts[j].getDictionary(); 
            }
            LOG.info("loaded. merging...");

            // merge the dictionary. this also produces the conversion map
            @SuppressWarnings("unchecked")
            Object mergedDict = originalTraits[i].mergeDictionary(baseDictsEntries, dictionaryConversion[i]);

            LOG.info("merged. writing...");
            @SuppressWarnings({ "rawtypes", "unchecked" })
            LocalDictFile newDictFile = new LocalDictFile(mergedDict, originalTraits[i]);
            newDictFile.writeToFile(newFiles[i].getDictionaryFile());
            newDictionaryBytesPerEntry[i] = newDictFile.getBytesPerEntry();

            newFiles[i].setDistinctValues(newDictFile.getDictionarySize());
            newFiles[i].setDictionaryBytesPerEntry(newDictFile.getBytesPerEntry());

            LOG.info("wrote.");
        }
        
        LOG.info("prepared!");
    }

    private final int cacheArrraySize = 1 << 16;

    /**
     * Simply append each base partition's data into the new file. This assumes the scheme has no sorting.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void copyBaseDataNoSorting (int col) throws IOException {
        ValueTraits traits = baseFilesReader[0][col].getCompressedDataTraits();
        ColumnFileBundle newFile = newFiles[col];
        TypedWriter dataWriter;
        if (compressions[col] == CompressionType.DICTIONARY) {
            dataWriter = new LocalFixLenWriter(newFile.getDataFile(), (FixLenValueTraits<?, ?>) traits);
        } else {
            dataWriter = LocalWriterFactory.getInstance(newFile, compressions[col], traits);
        }
        try {
            // simply copy from each file.
            for (int i = 0; i < basePartitionCount; ++i) {
                long start = System.currentTimeMillis();
                TypedReader baseDataReader = baseFilesReader[i][col].getCompressedDataReader();
                ValueTraits baseTraits = baseFilesReader[i][col].getCompressedDataTraits();

                try {
                    if (compressions[i] == CompressionType.DICTIONARY) {
                        copyBaseDataNoSortingDictionaryCompressed(col, i, dataWriter, baseDataReader, traits, baseTraits);
                    } else {
                        copyBaseDataNoSortingNonDictionary(col, i, dataWriter, baseDataReader, traits);
                    }
                } finally {
                    baseDataReader.close();
                }
                long end = System.currentTimeMillis();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("copied old data file (" + baseFiles[i][col].getDataFile().length() + " bytes) in " + (end - start) + "ms");
                }
            }
    
            long crc32Value = dataWriter.writeFileFooter();
            newFile.setDataFileChecksum(crc32Value);
            if (newFile.getPositionFile() != null) {
                dataWriter.writePositionFile(newFile.getPositionFile());
            }
            dataWriter.flush();
            // collect statistics
            assert (!(dataWriter instanceof TypedDictWriter));
            if (dataWriter instanceof TypedRLEWriter) {
                newFile.setRunCount(((TypedRLEWriter) dataWriter).getRunCount());
            } else if (dataWriter instanceof TypedBlockCmpWriter) {
                long uncompressedSize = ((TypedBlockCmpWriter) dataWriter).getTotalUncompressedSize();
                int uncompressedSizeKB = (int) (uncompressedSize / 1024L + (uncompressedSize % 1024 == 0 ? 0 : 1));
                newFile.setUncompressedSizeKB(uncompressedSizeKB);
            }
        } finally {
            dataWriter.close();
        }
    }

    /** Used from {@link #copyBaseDataNoSorting(int)} for columns that aren't dictionary-compressed. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void copyBaseDataNoSortingNonDictionary (int col, int base,
                    TypedWriter dataWriter, TypedReader baseDataReader,
                    ValueTraits traits) throws IOException {
        Object cacheArray = traits.createArray(cacheArrraySize);
        while (true) {
            int read = baseDataReader.readValues(cacheArray, 0, cacheArrraySize);
            if (read < 0) {
                break;
            }
            dataWriter.writeValues(cacheArray, 0, read);
        }
    }


    /**
     * Used from {@link #copyBaseDataNoSorting(int)} for columns that are dictionary-compressed.
     * Because of dictionary conversion, more tricky.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void copyBaseDataNoSortingDictionaryCompressed (int col, int base,
                    TypedWriter dataWriter, TypedReader baseDataReader,
                    ValueTraits traits, ValueTraits baseTraits) throws IOException {
        Object outCacheArray = traits.createArray(cacheArrraySize);
        Object inCacheArray;
        if (newDictionaryBytesPerEntry[base] != baseFiles[base][col].getDictionaryBytesPerEntry()) {
            // the base dictionary and the new dictionary has different size-per-entry. a bit tricky
            inCacheArray = baseTraits.createArray(cacheArrraySize);
        } else {
            inCacheArray = outCacheArray;
        }
        while (true) {
            int read = baseDataReader.readValues(inCacheArray, 0, cacheArrraySize);
            if (read < 0) {
                break;
            }
            // as it's dictionary-encoded, a little more conversion is needed
            if (outCacheArray == inCacheArray) {
                // convert the values using the old->new dictionary mapping
                if (outCacheArray instanceof byte[]) {
                    convertDictionaryCompressedValues ((byte[]) outCacheArray, 0, read, dictionaryConversion[col][base]);
                } else if (outCacheArray instanceof short[]) {
                    convertDictionaryCompressedValues ((short[]) outCacheArray, 0, read, dictionaryConversion[col][base]);
                } else {
                    convertDictionaryCompressedValues ((int[]) outCacheArray, 0, read, dictionaryConversion[col][base]);
                }
            } else {
                // further, the integer size is different (new one should be larger).
                // byte->short, byte->int or short->int
                if (inCacheArray instanceof short[]) {
                    resizeAndConvertDictionaryCompressedValues ((short[]) inCacheArray, (int[]) outCacheArray, 0, read, dictionaryConversion[col][base]);
                } else {
                    if (outCacheArray instanceof int[]) {
                        resizeAndConvertDictionaryCompressedValues ((byte[]) inCacheArray, (int[]) outCacheArray, 0, read, dictionaryConversion[col][base]);
                    } else {
                        resizeAndConvertDictionaryCompressedValues ((byte[]) inCacheArray, (short[]) outCacheArray, 0, read, dictionaryConversion[col][base]);
                    }
                }
            }
            dataWriter.writeValues(outCacheArray, 0, read);
        }
    }

    // followings are for the case where new/old dictionary use the same integer size
    private void convertDictionaryCompressedValues (byte[] values, int offset, int len, int[] conversion) {
        int adjustment = 1 << 7;
        for (int i = offset; i < offset + len; ++i) {
            assert (conversion[values[i] + adjustment] >= 0 && conversion[values[i] + adjustment] < 2 * adjustment);
            values[i] = (byte) (conversion[values[i] + adjustment] - adjustment);
        }
    }
    private void convertDictionaryCompressedValues (short[] values, int offset, int len, int[] conversion) {
        int adjustment = 1 << 15;
        for (int i = offset; i < offset + len; ++i) {
            assert (conversion[values[i] + adjustment] >= 0 && conversion[values[i] + adjustment] < 2 * adjustment);
            values[i] = (short) (conversion[values[i] + adjustment] - adjustment);
        }
    }
    private void convertDictionaryCompressedValues (int[] values, int offset, int len, int[] conversion) {
        for (int i = offset; i < offset + len; ++i) {
            values[i] = conversion[values[i] ^ 0x80000000] ^ 0x80000000;
        }
    }

    // followings are for the case where new/old dictionary use different integer size (new one must be larger as this is merging)
    private void resizeAndConvertDictionaryCompressedValues (byte[] before, short[] after, int offset, int len, int[] conversion) {
        for (int i = offset; i < offset + len; ++i) {
            assert (conversion[before[i] + (1 << 7)] >= 0 && conversion[before[i] + (1 << 7)] < (1 << 16));
            after[i] = (short) (conversion[before[i] + (1 << 7)] - (1 << 15));
        }
    }
    private void resizeAndConvertDictionaryCompressedValues (byte[] before, int[] after, int offset, int len, int[] conversion) {
        for (int i = offset; i < offset + len; ++i) {
            assert (conversion[before[i] + (1 << 7)] >= 0);
            after[i] = conversion[before[i] + (1 << 7)] ^ 0x80000000;
        }
    }
    private void resizeAndConvertDictionaryCompressedValues (short[] before, int[] after, int offset, int len, int[] conversion) {
        for (int i = offset; i < offset + len; ++i) {
            assert (conversion[before[i] + (1 << 15)] >= 0);
            after[i] = conversion[before[i] + (1 << 15)] ^ 0x80000000;
        }
    }

    /**
     * Merges each base partition's data for each column by the order of sorting column.
     */
    private void mergeSortedBaseData () throws IOException {
        //TODO
    }
}
