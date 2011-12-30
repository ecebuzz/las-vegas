package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Use {@link LocalDictFile}. this will be removed.
 */
public final class LocalStringDictFile implements OrderedDictionary<String, String[]> {
    private static Logger LOG = Logger.getLogger(LocalStringDictFile.class);

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
        // last, output it into the file
        writeToFile (dictFile, dict);
    }

    /**
     * Loads an existing dictionary file.
     */
    public LocalStringDictFile(VirtualFile dictFile) throws IOException {
        ValueTraits<String, String[]> traits = new AllValueTraits.VarcharValueTraits();
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
    public int getDictionarySize() {
        return dict.length;
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
    public void compressBatch(String[] src, int srcOff, byte[] dest, int destOff, int len) {
        throw new UnsupportedOperationException();
    }
    @Override
    public void compressBatch(String[] src, int srcOff, int[] dest, int destOff, int len) {
        throw new UnsupportedOperationException();
    }
    @Override
    public void compressBatch(String[] src, int srcOff, short[] dest, int destOff, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String decompress (int compresedValue) {
        int arrayIndex = convertCompressedValueToDictionaryIndex(compresedValue);
        assert (arrayIndex >= 0);
        assert (arrayIndex < dict.length);
        return dict[arrayIndex];
    }
    @Override
    public void decompressBatch(byte[] src, int srcOff, String[] dest, int destOff, int len) {
        throw new UnsupportedOperationException();
    }
    @Override
    public void decompressBatch(int[] src, int srcOff, String[] dest, int destOff, int len) {
        throw new UnsupportedOperationException();
    }
    @Override
    public void decompressBatch(short[] src, int srcOff, String[] dest, int destOff, int len) {
        throw new UnsupportedOperationException();
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
    
    @Override
    public void writeToFile (VirtualFile dictFile) throws IOException {
        writeToFile (dictFile, dict);
    }
    private static void writeToFile (VirtualFile dictFile, String[] theDict) throws IOException {
        long startMillisec = System.currentTimeMillis();
        // last, output it into the file
        ValueTraits<String, String[]> traits = new AllValueTraits.VarcharValueTraits();
        int fileSize = traits.getSerializedByteSize(theDict);
        if (fileSize > 1 << 26) {
            throw new IOException ("This dictionary will be too large: " + (fileSize >> 20) + "MB");
        }
        byte[] bytes = new byte[fileSize];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int writtenBytes = traits.serializeArray(theDict, byteBuffer);
        assert (bytes.length == writtenBytes);
        assert (byteBuffer.position() == writtenBytes);
        OutputStream out = dictFile.getOutputStream();
        out.write(bytes);
        out.flush();
        out.close();
        long endMillisec = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("Wrote out a dict file (" + dictFile.getAbsolutePath()
                            + "):" + theDict.length + " entries, " + dictFile.length()
                            + " bytes, in " + (endMillisec - startMillisec) + "ms");
        }
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
