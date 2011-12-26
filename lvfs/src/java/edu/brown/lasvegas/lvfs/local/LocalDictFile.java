package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Represents a dictionary file for dictionary compression.
 * 
 * <p>For most use-case, dictionary-encoding compresses a variable-length string column.
 * So, this class assumes String object as data.</p>
 * 
 * <p>So far, a dictionary file is just a serialized String[].
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
public final class LocalDictFile implements OrderedDictionary<String> {
    private static Logger LOG = Logger.getLogger(LocalDictFile.class);

    /**
     * Scans the given (uncompressed) data file and constructs a dictionary file. 
     * This method is not the fastest way to create dictionary-compressed file because
     * one needs to first write-out non-compressed file and then read it again.
     * For must faster file creation, use {@link LocalDictCompressionStringWriter}.
     * @param dictFile the dictionary file to be made
     * @param dataReader interface to read the data file
     */
    public static void createVarcharDictFile (VirtualFile dictFile, TypedReader<String, String[]> dataReader) throws IOException {
        LOG.info("Creating a dict file...");
        HashSet<String> distinctValues;
        {
            long startMillisec = System.currentTimeMillis();
            // first, scan the data file to get distinct values.
            // at this point, speed is much more important than memory.
            // so, we use HashSet (not TreeSet) with low load-factor for maximal speed
            distinctValues = new HashSet<String>(1 << 16, 0.25f);
            String[] buf = new String[1 << 12];
            while (true) {
                int read = dataReader.readValues(buf, 0, buf.length);
                for (int i = 0; i < read; ++i) {
                    distinctValues.add(buf[i]);
                }
                if (read == 0) {
                    break;
                }
            }
            long endMillisec = System.currentTimeMillis();
            LOG.info("scanned the data file in " + (endMillisec - startMillisec) + "ms");
        }
        String[] dict;
        {
            long startMillisec = System.currentTimeMillis();
            // second, construct the dictionary. This involves sorting.
            dict = distinctValues.toArray(new String[distinctValues.size()]);
            Arrays.sort(dict);
            if (LOG.isDebugEnabled()) {
                for (int i = 0; i < dict.length - 1; ++i) {
                    assert(!dict[i].equals(dict[i + 1]));
                }
            }
            long endMillisec = System.currentTimeMillis();
            LOG.info("sorted the dictionary in " + (endMillisec - startMillisec) + "ms");
        }
        {
            long startMillisec = System.currentTimeMillis();
            // last, output it into the file
            ObjectOutputStream out = new ObjectOutputStream(dictFile.getOutputStream());
            out.writeObject(dict);
            out.flush();
            out.close();
            long endMillisec = System.currentTimeMillis();
            LOG.info("Wrote out a dict file:" + dict.length + " entries, " + dictFile.length() + " bytes, in " + (endMillisec - startMillisec) + "ms");
        }
    }

    /**
     * Loads an existing dictionary file.
     */
    public LocalDictFile(VirtualFile dictFile) throws IOException {
        ObjectInputStream in = new ObjectInputStream(dictFile.getInputStream());
        try {
            dict = (String[]) in.readObject();
        } catch (Exception ex) {
            throw new IOException ("unexpected exception when loading a dictionary. corrupted dictionary?:" + dictFile.getAbsolutePath(), ex);
        }
        in.close();
        if (dict.length <= (1 << 8)) {
            bytesPerEntry = 1;
        } else if (dict.length <= (1 << 16)) {
            bytesPerEntry = 2;
        } else {
            bytesPerEntry = 4;
        }
        dictHashMap = new HashMap<String, Integer>(dict.length * 4); // large margin to reduce collisions. dictionary is anyway compact
        for (int i = 0; i < dict.length; ++i) {
            dictHashMap.put(dict[i], convertDictionaryIndexToCompressedValue(i));
        }
    }

    @Override
    public String[] getDictionary () {
        return dict;
    }
    @Override
    public byte getBytesPerEntry () {
        return bytesPerEntry;
    }
    @Override
    public Integer compress (String value) {
        return dictHashMap.get(value);
    }

    @Override
    public String decompress (int compresedValue) {
        int arrayIndex = convertCompressedValueToDictionaryIndex(compresedValue);
        assert (arrayIndex >= 0);
        assert (arrayIndex < dict.length);
        return dict[arrayIndex];
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
        assert (dictionaryIndex < dict.length);
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
    
    /**
     * Compresses the given file. 1-byte encoding version.
     * @param dataReader provides the data to be compressed
     * @param dataWriter receives the compressed data
     */
    public void compressVarcharFileByte (TypedReader<String, String[]> dataReader, TypedWriter<Byte, byte[]> dataWriter) throws IOException {
        assert (bytesPerEntry == 1);
        long startMillisec = System.currentTimeMillis();
        String[] buf = new String[1 << 12];
        byte[] compressed = new byte[buf.length];
        while (true) {
            int read = dataReader.readValues(buf, 0, buf.length);
            for (int i = 0; i < read; ++i) {
                compressed[i] = compress(buf[i]).byteValue();
            }
            if (read == 0) {
                break;
            }
            dataWriter.writeValues(compressed, 0, read);
        }
        long endMillisec = System.currentTimeMillis();
        LOG.info("compressed the data file in " + (endMillisec - startMillisec) + "ms");
    }
    /** Compresses the given file. 2-byte encoding version. */
    public void compressVarcharFileShort (TypedReader<String, String[]> dataReader, TypedWriter<Short, short[]> dataWriter) throws IOException {
        assert (bytesPerEntry == 2);
        long startMillisec = System.currentTimeMillis();
        String[] buf = new String[1 << 12];
        short[] compressed = new short[buf.length];
        while (true) {
            int read = dataReader.readValues(buf, 0, buf.length);
            for (int i = 0; i < read; ++i) {
                compressed[i] = compress(buf[i]).shortValue();
            }
            if (read == 0) {
                break;
            }
            dataWriter.writeValues(compressed, 0, read);
        }
        long endMillisec = System.currentTimeMillis();
        LOG.info("compressed the data file in " + (endMillisec - startMillisec) + "ms");
    }
    /** Compresses the given file. 4-byte encoding version. */
    public void compressVarcharFileInt (TypedReader<String, String[]> dataReader, TypedWriter<Integer, int[]> dataWriter) throws IOException {
        assert (bytesPerEntry == 4);
        long startMillisec = System.currentTimeMillis();
        String[] buf = new String[1 << 12];
        int[] compressed = new int[buf.length];
        while (true) {
            int read = dataReader.readValues(buf, 0, buf.length);
            for (int i = 0; i < read; ++i) {
                compressed[i] = compress(buf[i]);
            }
            if (read == 0) {
                break;
            }
            dataWriter.writeValues(compressed, 0, read);
        }
        long endMillisec = System.currentTimeMillis();
        LOG.info("compressed the data file in " + (endMillisec - startMillisec) + "ms");
    }
    
    /** 1/2/4 only.*/
    private final byte bytesPerEntry;
    /** stores all entries. entries are sorted by their values. */
    private final String[] dict;
    /**
     * for fast lookup on value (array is faster for lookup on index), we also maintain hashmap.
     * key=uncompressed-value, value=compressed-value.
     */
    private final HashMap<String, Integer> dictHashMap;
}
