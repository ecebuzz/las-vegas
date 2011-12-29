package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.local.LocalBlockCompressionFixLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalBlockCompressionVarLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalDictCompressionWriter;
import edu.brown.lasvegas.lvfs.local.LocalFixLenWriter;
import edu.brown.lasvegas.lvfs.local.LocalRLEWriter;
import edu.brown.lasvegas.lvfs.local.LocalVarLenWriter;

/**
 * Writers to write out a set of files which logically constitute a column file.
 */
public class ColumnFileWriterBundle {
    private TypedWriter<?, ?> dataWriter;
    
    private final VirtualFile outputFolder;
    /** filename without extension (e.g., "1_2_3" will generate "1_2_3.dat", "1_2_3.pos", and "1_2_3.dic"). */
    private final String fileNameSeed;
    /** data type BEFORE compression (data type after dictionary compression might be different!). */
    private final ColumnType columnType;
    private final CompressionType compressionType;
    private final ValueTraits<?, ?> traits;

    public ColumnFileWriterBundle (VirtualFile outputFolder, String fileNameSeed,
                    ColumnType columnType, CompressionType compressionType) throws IOException {
        this.outputFolder = outputFolder;
        this.fileNameSeed = fileNameSeed;
        this.columnType = columnType;
        this.compressionType = compressionType;
        this.traits = AllValueTraits.getInstance(columnType);
        this.dataWriter = instantiateWriter();
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private TypedWriter<?, ?> instantiateWriter () throws IOException {
        VirtualFile dataFile = outputFolder.getChildFile(LVFSFileType.DATA_FILE.appendExtension(fileNameSeed));
        switch (compressionType) {
        case DICTIONARY:
            return new LocalDictCompressionWriter(dataFile,
                outputFolder.getChildFile(LVFSFileType.DICTIONARY_FILE.appendExtension(fileNameSeed)),
                outputFolder.getChildFile(LVFSFileType.TMP_DATA_FILE.appendExtension(fileNameSeed)), traits);
        case RLE:
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
}
