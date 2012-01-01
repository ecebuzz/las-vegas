package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.PositionIndex;
import edu.brown.lasvegas.lvfs.TypedDictReader;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.FixLenValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * Implementation of {@link TypedDictReader}.
 * This forwards basically everything to the internal compressedReader except that
 * it de-compresses the integer values to the original data type.
 */
public class LocalDictCompressionReader<T extends Comparable<T>, AT, CT extends Number & Comparable<CT>, CAT>
    implements TypedDictReader<T, AT, CT, CAT> {
    private static Logger LOG = Logger.getLogger(LocalDictCompressionReader.class);

    /** the internal integer file reader BEFORE dictionary decompression. */
    private final LocalFixLenReader<CT, CAT> compressedReader;
    
    /**
     * number of byte to represent one entry after dictionary compression.
     */
    private final byte compressedBytesPerValue;

    /** traits for the original data type (AFTER de-compression). */
    private final ValueTraits<T, AT> originalTraits;

    /** traits for the compressed integer data type (BEFORE de-compression). */
    private final FixLenValueTraits<CT, CAT> compressedTraits;
    
    /** loaded dictionary file. It's loaded lazily to help in-situ (no de-compression) data processing. */
    private OrderedDictionary<T, AT> dict;
    
    /** dictionary file. */
    private final VirtualFile dictionaryFile;

    public LocalDictCompressionReader(VirtualFile compressedFile, FixLenValueTraits<CT, CAT> compressedTraits, VirtualFile dictionaryFile, ValueTraits<T, AT> originalTraits, int streamBufferSize) throws IOException {
        this.compressedReader = new LocalFixLenReader<CT, CAT> (compressedFile, compressedTraits, streamBufferSize);
        this.compressedBytesPerValue = (byte) (compressedTraits.getBitsPerValue() / 8);
        assert (compressedBytesPerValue == 1 || compressedBytesPerValue == 2 || compressedBytesPerValue == 4);
        this.compressedTraits = compressedTraits;
        this.originalTraits = originalTraits;
        this.dictionaryFile = dictionaryFile;
        assert (dictionaryFile != null);
    }
    public LocalDictCompressionReader(VirtualFile compressedFile, FixLenValueTraits<CT, CAT> compressedTraits, VirtualFile dictionaryFile, ValueTraits<T, AT> originalTraits) throws IOException {
        this (compressedFile, compressedTraits, dictionaryFile, originalTraits, 1 << 16);
    }
    @SuppressWarnings("unchecked")
    public LocalDictCompressionReader(VirtualFile compressedFile, byte bytesPerEntry, VirtualFile dictionaryFile, ValueTraits<T, AT> originalTraits, int streamBufferSize) throws IOException {
        this (compressedFile, (FixLenValueTraits<CT, CAT>) ValueTraitsFactory.getIntegerTraits (bytesPerEntry), dictionaryFile, originalTraits, streamBufferSize);
    }
    @SuppressWarnings("unchecked")
    public LocalDictCompressionReader(VirtualFile compressedFile, byte bytesPerEntry, VirtualFile dictionaryFile, ValueTraits<T, AT> originalTraits) throws IOException {
        this (compressedFile, (FixLenValueTraits<CT, CAT>) ValueTraitsFactory.getIntegerTraits  (bytesPerEntry), dictionaryFile, originalTraits, 1 << 16);
    }
    @Override
    public ValueTraits<T, AT> getValueTraits() {
        return originalTraits;
    }
    
    @Override
    public TypedReader<CT, CAT> getCompressedReader() {
        return compressedReader;
    }
    
    @Override
    public void loadPositionIndex(PositionIndex posIndex) throws IOException {
        compressedReader.loadPositionIndex(posIndex);
    }
    
    @Override
    public void close() throws IOException {
        compressedReader.close();
    }

    @Override
    public void loadDict () throws IOException {
        if (this.dict != null) {
            return;
        }
        LocalDictFile<T, AT> theDict = new LocalDictFile<T, AT>(dictionaryFile, originalTraits);
        LOG.info("loaded dictionary");
        if (theDict.getBytesPerEntry() != compressedBytesPerValue) {
            throw new IOException ("size of compressed integer values doesn't match. probably the constructor parameter (traits/bytesPerEntry) was wrong.");
        }
        this.dict = theDict;
    }

    @Override
    public OrderedDictionary<T, AT> getDict() throws IOException {
        if (dict == null) {
            loadDict ();
        }
        return dict;
    }

    @Override
    public T readValue() throws IOException {
        if (dict == null) {
            loadDict ();
        }
        CT cv = readCompressedValue();
        return dict.decompress(cv.intValue());
    }
    
    private CAT catBuffer;
    private int catBufferLength = 0;
    private void assureCATBuffer (int size) {
        if (catBuffer == null || catBufferLength < size) {
            int newLen = 10 + size * 12 / 10;
            catBuffer = compressedTraits.createArray(newLen);
            catBufferLength = newLen;
        }
    }

    @Override
    public int readValues(AT buffer, int off, int len) throws IOException {
        if (dict == null) {
            loadDict ();
        }
        assureCATBuffer(len);
        int read = readCompressedValues(catBuffer, 0, len);
        if (catBuffer instanceof byte[]) {
            dict.decompressBatch((byte[]) catBuffer, 0, buffer, off, len);
        } else if (catBuffer instanceof short[]) {
            dict.decompressBatch((short[]) catBuffer, 0, buffer, off, len);
        } else {
            dict.decompressBatch((int[]) catBuffer, 0, buffer, off, len);
        }
        return read;
    }

    @Override
    public void skipValue() throws IOException {
        compressedReader.skipValue();
    }

    @Override
    public void skipValues(int skip) throws IOException {
        compressedReader.skipValues(skip);
    }

    @Override
    public void seekToTupleAbsolute(int tuple) throws IOException {
        compressedReader.seekToTupleAbsolute(tuple);
    }

    @Override
    public int getTotalTuples() {
        return compressedReader.getTotalTuples();
    }

    @Override
    public FixLenValueTraits<CT, CAT> getCompressedValueTraits() {
        return compressedTraits;
    }

    @Override
    public CT readCompressedValue() throws IOException {
        return compressedReader.readValue();
    }

    @Override
    public int readCompressedValues(CAT buffer, int off, int len) throws IOException {
        return compressedReader.readValues(buffer, off, len);
    }
    
    @Override
    public int readCompressedValueInt() throws IOException {
        return compressedReader.readValue().intValue();
    }
    
    @Override
    public int readCompressedValuesInt(int[] buffer, int off, int len) throws IOException {
        if (compressedBytesPerValue == 4) {
            @SuppressWarnings("unchecked")
            int read = compressedReader.readValues((CAT) buffer, off, len);
            return read;
        }
        
        assureCATBuffer(len);
        int read = compressedReader.readValues(catBuffer, 0, len);
        if (catBuffer instanceof byte[]) {
            byte[] casted = (byte[]) catBuffer;
            for (int i = 0; i < read; ++i) {
                buffer[off + i] = casted[i];
            }
        } else {
            short[] casted = (short[]) catBuffer;
            for (int i = 0; i < read; ++i) {
                buffer[off + i] = casted[i];
            }
        }
        return read;
    }
}
