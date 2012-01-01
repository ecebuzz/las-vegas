package edu.brown.lasvegas.lvfs;

import java.io.Closeable;
import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.local.LocalBlockCompressionFixLenReader;
import edu.brown.lasvegas.lvfs.local.LocalBlockCompressionVarLenReader;
import edu.brown.lasvegas.lvfs.local.LocalDictCompressionReader;
import edu.brown.lasvegas.lvfs.local.LocalFixLenReader;
import edu.brown.lasvegas.lvfs.local.LocalPosFile;
import edu.brown.lasvegas.lvfs.local.LocalRLEReader;
import edu.brown.lasvegas.lvfs.local.LocalValFile;
import edu.brown.lasvegas.lvfs.local.LocalVarLenReader;
import edu.brown.lasvegas.traits.FixLenValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.traits.IntegerValueTraits;
import edu.brown.lasvegas.traits.SmallintValueTraits;
import edu.brown.lasvegas.traits.TinyintValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.VarLenValueTraits;

/**
 * Readers to read a set of files which logically constitute a column.
 */
public final class ColumnFileReaderBundle implements Closeable {
    /** the columnar file. */
    private final ColumnFileBundle fileBundle;
    /** traits for data type in the main data file. this might be different from the original data type because of dictionary compression. */
    private final ValueTraits<?, ?> compressedDataTraits;
    /** traits for original data type. */
    private final ValueTraits<?, ?> originalDataTraits;

    /** reader for main data file. */
    private TypedReader<?, ?> dataReader;

    /** the position index, which is loaded lazily. */
    private PositionIndex positionIndex;
    /** the value index, which is loaded lazily. */
    private ValueIndex<?> valueIndex;
    
    /**
     * Instantiate a reader bundle. This doesn't open any file at this point
     * because some file might not be required.
     */
    public ColumnFileReaderBundle (ColumnFileBundle fileBundle) {
        this.fileBundle = fileBundle;
        this.originalDataTraits = ValueTraitsFactory.getInstance(fileBundle.getColumnType());
        if (fileBundle.getCompressionType() == CompressionType.DICTIONARY) {
            // dictionary encoding changes the data type in main data file to be 1/2/4 integers
            switch (fileBundle.getDictionaryBytesPerEntry()) {
            case 1: 
                this.compressedDataTraits = new TinyintValueTraits();
                break;
            case 2: 
                this.compressedDataTraits = new SmallintValueTraits();
                break;
            default: 
                assert (fileBundle.getDictionaryBytesPerEntry() == 4);
                this.compressedDataTraits = new IntegerValueTraits();
                break;
            }
        } else {
            this.compressedDataTraits = originalDataTraits;
        }
    }
    
    /**
     * Gets the reader for main data file.
     *
     * @return the reader for main data file
     */
    public TypedReader<?, ?> getDataReader () throws IOException {
        if (dataReader == null) {
            dataReader = instantiateDataReader();
            if (positionIndex != null) {
                dataReader.loadPositionIndex(positionIndex);
            }
        }
        return dataReader;
    }
    
    /**
     * Gets the reader for main data file WITHOUT decompression if it's dictionary-encoded.
     * If it's not dictionary-encoded, same as getDataReader() because internal data type is unchanged (even in RLE).
     * @return the reader for main data file WITHOUT decompression if it's dictionary-encoded
     */
    @SuppressWarnings("rawtypes")
    public TypedReader<?, ?> getCompressedDataReader() throws IOException {
        TypedReader<?, ?> reader = getDataReader();
        if (reader instanceof TypedDictReader) {
            return ((TypedDictReader) reader).getCompressedReader(); 
        }
        return reader;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private TypedReader<?, ?> instantiateDataReader () throws IOException {
        switch (fileBundle.getCompressionType()) {
        case DICTIONARY:
            return new LocalDictCompressionReader(fileBundle.getDataFile(), (FixLenValueTraits<?, ?>) compressedDataTraits, fileBundle.getDictionaryFile(), originalDataTraits);
        case RLE:
            return new LocalRLEReader(fileBundle.getDataFile(), originalDataTraits);
        case GZIP_BEST_COMPRESSION:
        case SNAPPY:
            if (originalDataTraits instanceof VarLenValueTraits<?>) {
                return new LocalBlockCompressionVarLenReader(fileBundle.getDataFile(), (VarLenValueTraits<?>) originalDataTraits, fileBundle.getCompressionType());
            } else {
                return new LocalBlockCompressionFixLenReader(fileBundle.getDataFile(), (FixLenValueTraits<?, ?>) originalDataTraits, fileBundle.getCompressionType());
            }
        case NONE:
            if (originalDataTraits instanceof VarLenValueTraits<?>) {
                return new LocalVarLenReader(fileBundle.getDataFile(), (VarLenValueTraits<?>) originalDataTraits);
            } else {
                return new LocalFixLenReader(fileBundle.getDataFile(), (FixLenValueTraits<?, ?>) originalDataTraits);
            }
        default:
            throw new IllegalArgumentException("unexpected compression type:" + fileBundle.getCompressionType());
        }
    }
    
    /**
     * Gets the position index, which is loaded lazily.
     *
     * @return the position index, which is loaded lazily
     */
    public PositionIndex getPositionIndex () throws IOException {
        if (positionIndex == null && fileBundle.getPositionFile() != null) {
            positionIndex = new LocalPosFile(fileBundle.getPositionFile());
            if (dataReader != null) {
                dataReader.loadPositionIndex(positionIndex);
            }
        }
        return positionIndex;
    }
    
    /**
     * Gets the dictionary, which is loaded lazily.
     *
     * @return the dictionary, which is loaded lazily
     */
    @SuppressWarnings({ "rawtypes" })
    public OrderedDictionary<?, ?> getDictionary () throws IOException {
        return ((TypedDictReader) getDataReader()).getDict();
    }
    
    /**
     * Gets the value index, which is loaded lazily.
     *
     * @return the value index, which is loaded lazily
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ValueIndex<?> getValueIndex () throws IOException {
        if (valueIndex == null && fileBundle.getValueFile() != null) {
            valueIndex = new LocalValFile(fileBundle.getValueFile(), originalDataTraits);
        }
        return valueIndex;
    }
    
    
    @Override
    public void close() throws IOException {
        if (dataReader != null) {
            dataReader.close();
        }
    }

    /**
     * Gets the traits for data type in the main data file.
     *
     * @return the traits for data type in the main data file
     */
    public ValueTraits<?, ?> getCompressedDataTraits() {
        return compressedDataTraits;
    }

    /**
     * Gets the traits for original data type.
     *
     * @return the traits for original data type
     */
    public ValueTraits<?, ?> getOriginalDataTraits() {
        return originalDataTraits;
    }
}
