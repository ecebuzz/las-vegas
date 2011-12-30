package edu.brown.lasvegas.lvfs;

import java.io.Closeable;
import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.local.LocalBlockCompressionFixLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalBlockCompressionVarLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalDictCompressionWriter;
import edu.brown.lasvegas.lvfs.local.LocalFixLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalRLEWriter;
import edu.brown.lasvegas.lvfs.local.LocalVarLenWriter;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.traits.ValueTraits;

/**
 * Writers to write out a set of files which logically constitute a column file.
 */
public final class ColumnFileWriterBundle implements Closeable {
    private TypedWriter<?, ?> dataWriter;
    
    private final VirtualFile outputFolder;
    /** filename without extension (e.g., "1_2_3" will generate "1_2_3.dat", "1_2_3.pos", and "1_2_3.dic"). */
    private final String fileNameSeed;
    /** data type BEFORE compression (data type after dictionary compression might be different!). */
    private final ColumnType columnType;
    private final CompressionType compressionType;
    private final ValueTraits<?, ?> traits;

    private VirtualFile dataFile;
    private long dataFileChecksum;
    private VirtualFile dictionaryFile;
    private VirtualFile positionFile;
    private VirtualFile valueFile;
    
    /** The size of one entry after dictionary-compression (1/2/4), Set only when the column file is dictionary-compressed (otherwise 0).*/
    private byte dictionaryBytesPerEntry = 0;
    
    /** The number of distinct values in this file, Set only when the column file is dictionary-compressed or sorted (otherwise 0). */
    private int distinctValues = 0;
    
    /** The average run length in this file, Set only when the column file is RLE-compressed (otherwise 0).*/
    private int runCount = 0;
    
    /** file size without gzip/snappy compression in KB.  Set only when the column file is GZIP/SNAPPY-compressed (otherwise 0). */
    private int uncompressedSizeKB;

    public ColumnFileWriterBundle (VirtualFile outputFolder, String fileNameSeed,
                    ColumnType columnType, CompressionType compressionType, boolean calculateChecksum) throws IOException {
        this.outputFolder = outputFolder;
        this.fileNameSeed = fileNameSeed;
        this.columnType = columnType;
        this.compressionType = compressionType;
        this.traits = ValueTraitsFactory.getInstance(columnType);
        this.dataWriter = instantiateWriter();
        if (calculateChecksum) {
            dataWriter.setCRC32Enabled(true);
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private TypedWriter<?, ?> instantiateWriter () throws IOException {
        dataFile = outputFolder.getChildFile(LVFSFileType.DATA_FILE.appendExtension(fileNameSeed));
        switch (compressionType) {
        case DICTIONARY:
            dictionaryFile = outputFolder.getChildFile(LVFSFileType.DICTIONARY_FILE.appendExtension(fileNameSeed));
            return new LocalDictCompressionWriter(dataFile, dictionaryFile,
                outputFolder.getChildFile(LVFSFileType.TMP_DATA_FILE.appendExtension(fileNameSeed)), traits);
        case RLE:
            positionFile = outputFolder.getChildFile(LVFSFileType.POSITION_FILE.appendExtension(fileNameSeed));
            return new LocalRLEWriter(dataFile, traits);
        case GZIP_BEST_COMPRESSION:
        case SNAPPY:
            if (traits instanceof VarLenValueTraits<?>) {
                return new LocalBlockCompressionVarLenWriter(dataFile, (VarLenValueTraits<?>) traits, compressionType);
            } else {
                return new LocalBlockCompressionFixLenWriter(dataFile, (FixLenValueTraits<?, ?>) traits, compressionType);
            }
        case NONE:
            if (traits instanceof VarLenValueTraits<?>) {
                return new LocalVarLenWriter(dataFile, (VarLenValueTraits<?>) traits);
            } else {
                return new LocalFixLenWriter(dataFile, (FixLenValueTraits<?, ?>) traits);
            }
        default:
            throw new IllegalArgumentException("unexpected compression type:" + compressionType);
        }
    }
    
    public void finish() throws IOException {
        dataFileChecksum = dataWriter.writeFileFooter();
        dataWriter.flush();
        if (positionFile != null) {
            dataWriter.writePositionFile(positionFile);
        }

        // collect statistics
        switch (compressionType) {
        case DICTIONARY:
        {
            OrderedDictionary<?, ?> dict = ((TypedDictWriter<?, ?>) dataWriter).getFinalDict();
            dictionaryBytesPerEntry = dict.getBytesPerEntry();
            distinctValues = dict.getDictionarySize();
            break;
        }
        case RLE:
            runCount = ((TypedRLEWriter<?, ?>) dataWriter).getRunCount();
            break;
        case GZIP_BEST_COMPRESSION:
        case SNAPPY:
        {
            long uncompressedSize = ((TypedBlockCmpWriter<?, ?>) dataWriter).getTotalUncompressedSize();
            uncompressedSizeKB = (int) (uncompressedSize / 1024L) + (uncompressedSize % 1024 == 0 ? 0 : 1);
        }
            break;
        }
    }
    
    @Override
    public void close() throws IOException {
        if (dataWriter != null) {
            dataWriter.close();
        }
    }

    public TypedWriter<?, ?> getDataWriter() {
        return dataWriter;
    }

    public VirtualFile getOutputFolder() {
        return outputFolder;
    }

    public String getFileNameSeed() {
        return fileNameSeed;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public ValueTraits<?, ?> getTraits() {
        return traits;
    }

    public VirtualFile getDataFile() {
        return dataFile;
    }

    public long getDataFileChecksum() {
        return dataFileChecksum;
    }

    public VirtualFile getDictionaryFile() {
        return dictionaryFile;
    }

    public VirtualFile getPositionFile() {
        return positionFile;
    }

    public VirtualFile getValueFile() {
        return valueFile;
    }

    public byte getDictionaryBytesPerEntry() {
        return dictionaryBytesPerEntry;
    }

    public int getDistinctValues() {
        return distinctValues;
    }

    public int getRunCount() {
        return runCount;
    }

    public int getUncompressedSizeKB() {
        return uncompressedSizeKB;
    }
}
