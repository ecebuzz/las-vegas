package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Represents a dictionary file for dictionary compression.
 * 
 * <p>So far, a dictionary file is just a serialized value array.
 * We store and load them at once. Simple and fast.
 * Hard to imagine other use of this file.</p>
 * 
 * <p>All dictionaries are order-preserving, meaning a compressed value keeps the
 * less-than and greater-than relationship (of course in addition to equal-to).
 * For example, if the compressed value of a column is 10 and another compressed value
 * if 20, the decompressed value of former is strictly smaller than that of latter.</p>
 * 
 * <p>Compressed values are 1/2/4 signed integers (byte/short/int). To exploit (literally)
 * the last bit, the dictionary numbering starts from negative values. For example,
 * in 2-byte dictionary, the compressed value are -32768,-32767,... Be VERY careful
 * dealing with this if you call {@link #getDictionary()} and directly use the internal
 * dictionary data. Usually, the conversion is done by {@link #convertCompressedValueToDictionaryIndex(int)}.</p>
 */
public final class LocalDictFile<T extends Comparable<T>, AT> implements OrderedDictionary<T, AT> {
    private static Logger LOG = Logger.getLogger(LocalDictFile.class);

    /**
     * Scans the given (uncompressed) data file and constructs a dictionary file. 
     * This method is not the fastest way to create dictionary-compressed file because
     * one needs to first write-out non-compressed file and then read it again.
     * For must faster file creation, use {@link LocalDictCompressionWriter}.
     * @param dataReader interface to read the data file
     * @param type value type BEFORE compression
     */
    @SuppressWarnings("unchecked")
    public LocalDictFile(TypedReader<T, AT> dataReader, ColumnType type) throws IOException {
        this (dataReader, (ValueTraits<T, AT>) AllValueTraits.getInstance(type));
    }
    public LocalDictFile(TypedReader<T, AT> dataReader, ValueTraits<T, AT> traits) throws IOException {
        LOG.info("Creating a dictionary...");
        this.traits = traits;
        HashSet<T> distinctValues;
        {
            long startMillisec = System.currentTimeMillis();
            // first, scan the data file to get distinct values.
            // at this point, speed is much more important than memory.
            // so, we use HashSet (not TreeSet) with low load-factor for maximal speed
            distinctValues = new HashSet<T>(1 << 16, 0.25f);
            final int BUF_SIZE = 1 << 12;
            AT buf = traits.createArray(BUF_SIZE);
            while (true) {
                int read = dataReader.readValues(buf, 0, BUF_SIZE);
                for (int i = 0; i < read; ++i) {
                    distinctValues.add(traits.get(buf, i));
                }
                if (read == 0) {
                    break;
                }
            }
            long endMillisec = System.currentTimeMillis();
            LOG.info("scanned the data file in " + (endMillisec - startMillisec) + "ms");
        }
        {
            long startMillisec = System.currentTimeMillis();
            // second, construct the dictionary. This involves sorting.
            this.dict = traits.toArray(distinctValues);
            traits.sort(dict);
            this.dictEntryCount = traits.length(dict);
            this.bytesPerEntry = calculateBytesPerEntry(dictEntryCount);
            if (LOG.isDebugEnabled()) {
                for (int i = 0; i < dictEntryCount - 1; ++i) {
                    assert(traits.get(dict, i).compareTo(traits.get(dict, i + 1)) < 0);
                }
            }
            long endMillisec = System.currentTimeMillis();
            LOG.info("sorted the dictionary in " + (endMillisec - startMillisec) + "ms");
        }
        assert(validateDict ());
    }

    /**
     * Creates a dictionary with sorted data.
     * @param type value type BEFORE compression
     */
    @SuppressWarnings("unchecked")
    public LocalDictFile(AT dict, ColumnType type) throws IOException {
        this (dict, (ValueTraits<T, AT>) AllValueTraits.getInstance(type));
    }
    public LocalDictFile(AT dict, ValueTraits<T, AT> traits) throws IOException {
        this.dict = dict;
        this.traits = traits;
        this.dictEntryCount = traits.length(dict);
        this.bytesPerEntry = calculateBytesPerEntry(dictEntryCount);
        assert(validateDict ());
    }

    /**
     * Loads an existing dictionary file.
     * @param type value type BEFORE compression
     */
    @SuppressWarnings("unchecked")
    public LocalDictFile(VirtualFile dictFile, ColumnType type) throws IOException {
        this (dictFile, (ValueTraits<T, AT>) AllValueTraits.getInstance(type));
    }
    public LocalDictFile(VirtualFile dictFile, ValueTraits<T, AT> traits) throws IOException {
        this.traits = traits;
        int fileSize = (int) dictFile.length();
        if (fileSize > 1 << 26) {
            throw new IOException ("This dictionary seems too large: " + dictFile.getAbsolutePath() + ": " + (dictFile.length() >> 20) + "MB");
        }
        byte[] bytes = new byte[fileSize];
        InputStream in = dictFile.getInputStream();
        int read = in.read(bytes);
        in.close();
        assert (read == bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            dict = traits.deserializeArray(buffer);
        } catch (IOException ex) {
            throw new IOException ("unexpected exception when loading a dictionary. corrupted dictionary?:" + dictFile.getAbsolutePath(), ex);
        }
        assert (buffer.position() == bytes.length);
        this.dictEntryCount = traits.length(dict);
        this.bytesPerEntry = calculateBytesPerEntry(dictEntryCount);
        assert(validateDict ());
    }
    private static byte calculateBytesPerEntry (int entryCount) {
        if (entryCount <= (1 << 8)) {
            return 1;
        } else if (entryCount <= (1 << 16)) {
            return 2;
        } else {
            return 4;
        }
    }
    /** used for assertion. not supposed to be fast. */
    private boolean validateDict () {
        if (LOG.isDebugEnabled()) {
            for (int i = 1; i < dictEntryCount; ++i) {
                T prev = traits.get(dict, i - 1);
                T cur = traits.get(dict, i);
                assert (prev.compareTo(cur) < 0);
            }
        }
        return true;
    }

    @Override
    public AT getDictionary () {
        return dict;
    }
    @Override
    public int getDictionarySize () {
        return dictEntryCount;
    }
    @Override
    public byte getBytesPerEntry () {
        return bytesPerEntry;
    }
    @Override
    public Integer compress (T value) {
        int arrayIndex = traits.binarySearch(dict, value);
        if (arrayIndex < 0) {
            return null; // not found
        }
        return convertDictionaryIndexToCompressedValue(arrayIndex);
    }
    @Override
    public void compressBatch(AT src, int srcOff, byte[] dest, int destOff, int len) {
        assert (bytesPerEntry == 1);
        assert (dest.length >= destOff + len);
        assert (traits.length(src) >= srcOff + len);
        for (int i = 0; i < len; ++i) {
            int arrayIndex = traits.binarySearch(dict, traits.get(src, srcOff + i));
            dest[destOff + i] = (byte) (arrayIndex - (1 << 7));
        }
    }
    @Override
    public void compressBatch(AT src, int srcOff, short[] dest, int destOff, int len) {
        assert (bytesPerEntry == 2);
        assert (dest.length >= destOff + len);
        assert (traits.length(src) >= srcOff + len);
        for (int i = 0; i < len; ++i) {
            int arrayIndex = traits.binarySearch(dict, traits.get(src, srcOff + i));
            dest[destOff + i] = (short) (arrayIndex - (1 << 15));
        }
    }
    @Override
    public void compressBatch(AT src, int srcOff, int[] dest, int destOff, int len) {
        assert (bytesPerEntry == 4);
        assert (dest.length >= destOff + len);
        assert (traits.length(src) >= srcOff + len);
        for (int i = 0; i < len; ++i) {
            int arrayIndex = traits.binarySearch(dict, traits.get(src, srcOff + i));
            dest[destOff + i] = arrayIndex ^ 0x80000000;
        }
    }

    @Override
    public T decompress (int compresedValue) {
        int arrayIndex = convertCompressedValueToDictionaryIndex(compresedValue);
        assert (arrayIndex >= 0);
        assert (arrayIndex < dictEntryCount);
        return traits.get(dict, arrayIndex);
    }
    
    @Override
    public void decompressBatch(byte[] src, int srcOff, AT dest, int destOff, int len) {
        assert (bytesPerEntry == 1);
        assert (traits.length(dest) >= destOff + len);
        assert (src.length >= srcOff + len);
        for (int i = 0; i < len; ++i) {
            T value = traits.get(dict, src[srcOff + i] + (1 << 7));
            traits.set(dest, destOff + i, value);
        }
    }

    @Override
    public void decompressBatch(short[] src, int srcOff, AT dest, int destOff, int len) {
        assert (bytesPerEntry == 2);
        assert (traits.length(dest) >= destOff + len);
        assert (src.length >= srcOff + len);
        for (int i = 0; i < len; ++i) {
            T value = traits.get(dict, src[srcOff + i] + (1 << 15));
            traits.set(dest, destOff + i, value);
        }
    }

    @Override
    public void decompressBatch(int[] src, int srcOff, AT dest, int destOff, int len) {
        assert (bytesPerEntry == 4);
        assert (traits.length(dest) >= destOff + len);
        assert (src.length >= srcOff + len);
        for (int i = 0; i < len; ++i) {
            T value = traits.get(dict, src[srcOff + i] ^ 0x80000000);
            traits.set(dest, destOff + i, value);
        }
    }

    @Override
    public int convertCompressedValueToDictionaryIndex (int compresedValue) {
        switch (bytesPerEntry) {
        case 1:
            assert (compresedValue < 1 << 7);
            assert (compresedValue >= -(1 << 7));
            return compresedValue + (1 << 7);
        case 2:
            assert (compresedValue < 1 << 15);
            assert (compresedValue >= -(1 << 15));
            return compresedValue + (1 << 15);
        case 4:
            return compresedValue ^ 0x80000000;
        default:
            assert (false);
            return -1;
        }
    }
    @Override
    public int convertDictionaryIndexToCompressedValue (int dictionaryIndex) {
        assert (dictionaryIndex >= 0);
        assert (dictionaryIndex < dictEntryCount);
        switch (bytesPerEntry) {
        case 1:
            return dictionaryIndex - (1 << 7);
        case 2:
            return dictionaryIndex - (1 << 15);
        case 4:
            return dictionaryIndex ^ 0x80000000;
        default:
            assert (false);
            return -1;
        }
    }
    
    @Override
    public void writeToFile (VirtualFile dictFile) throws IOException {
        long startMillisec = System.currentTimeMillis();
        // output it into the file
        int fileSize = traits.getSerializedByteSize(dict);
        if (fileSize > 1 << 26) {
            throw new IOException ("This dictionary will be too large: " + (fileSize >> 20) + "MB");
        }
        byte[] bytes = new byte[fileSize];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int writtenBytes = traits.serializeArray(dict, byteBuffer);
        assert (bytes.length == writtenBytes);
        assert (byteBuffer.position() == writtenBytes);
        OutputStream out = dictFile.getOutputStream();
        out.write(bytes);
        out.flush();
        out.close();
        long endMillisec = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("Wrote out a dict file (" + dictFile.getAbsolutePath()
                            + "):" + dictEntryCount + " entries, " + dictFile.length()
                            + " bytes, in " + (endMillisec - startMillisec) + "ms");
        }
    }

    /** 1/2/4 only.*/
    private final byte bytesPerEntry;
    /** stores all entries. entries are sorted by their values. */
    private final AT dict;
    /** dict.length. */
    private final int dictEntryCount;
    private final ValueTraits<T, AT> traits;
}
