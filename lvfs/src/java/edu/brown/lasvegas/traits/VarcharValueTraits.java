package edu.brown.lasvegas.traits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collection;

import edu.brown.lasvegas.lvfs.RawValueReader;
import edu.brown.lasvegas.lvfs.RawValueWriter;
import edu.brown.lasvegas.lvfs.TypedRLEWriter;
import edu.brown.lasvegas.lvfs.ValueRun;
import edu.brown.lasvegas.util.KeyValueArrays;

/**
 * Traits for variable-length char (java-String).
 */
public final class VarcharValueTraits implements VarLenValueTraits<String> {
    @Override
    public String readValue(RawValueReader reader) throws IOException {
        return reader.readStringWithLengthHeader();
    }
    @Override
    public void writeValue(RawValueWriter writer, String value) throws IOException {
        writer.writeBytesWithLengthHeader(value.getBytes(RawValueWriter.CHARSET));
    }
    @Override
    public void writeRunLengthes(TypedRLEWriter<String, String[]> writer, String[] values, int off, int len) throws IOException {
        ValueRun<String> cur = writer.getCurrentRun();
        String curValue = cur.value;
        for (int i = off; i < off + len; ++i) {
            if (values[i].equals(curValue)) {
                ++cur.runLength;
            } else {
                cur = writer.startNewRun(values[i], 1);
                curValue = values[i];
            }
        }
    }
    @Override
    public String[] createArray(int size) {
        return new String[size];
    }
    @Override
    public String[][] create2DArray(int size) {
        return new String[size][];
    }
    @Override
    public int length(String[] array) {
        return array.length;
    }
    @Override
    public String[] toArray(Collection<String> values) {
        return values.toArray(createArray(values.size()));
    }
    @Override
    public int binarySearch(String[] array, String value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(String[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(String[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(String[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(String[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    @Override
    public String[] reorder(String[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        String[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    @Override
    public int countDistinct(String[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        String prev = array[0];
        int distinctValues = 1;
        for (int i = 1; i < len; ++i) {
            if (!array[i].equals(prev)) {
                ++distinctValues;
                prev = array[i];
            }
        }
        return distinctValues;
    }
    @Override
    public void fillArray(String value, String[] array, int off, int len) {
        Arrays.fill(array, off, off + len, value);
    }
    @Override
    public String get(String[] array, int index) {
        return array[index];
    }
    @Override
    public void set(String[] array, int index, String value) {
        array[index] = value;
    }
    private int getMax (int[] values) {
    	int ret = Integer.MIN_VALUE;
    	for (int value : values) {
    		ret = value > ret ? value : ret;
    	}
    	return ret;
    }
    
    @Override
    public String[] deserializeArray(ByteBuffer buffer) throws IOException {
        // Unlike the fixed-length data types, VARCHAR/VARBIN is a little bit tricky.
        // To make serialize/deserialize low-overhead, we use the following format.
        // The data has two parts; integer length part and character part.
        
        // The length part is merely an array of int.
        // First int is the number of entries, let's says it's n.
        // It's followed by n ints, length of each String. (-1=null)

        // The second part is a concatenated array of char (via java.nio.CharBuffer).
        // As this is (sort of) UTF-16 encoded for faster serialize/deserialize,
        // the serialized data might be larger than UTF-8 encoded data.
        // But, assuming this method is used for a relatively small array, it's fine.
        // Also, for some language, UTF-16 might result in smaller data too!

        int entries = buffer.getInt();
        assert (entries >= -1);
        if (entries == -1) return null;

        IntBuffer lengthBuffer = buffer.asIntBuffer();
        int[] lengthes = new int[entries];
        lengthBuffer.get(lengthes);
        buffer.position(buffer.position() + entries * 4); // advance original buffer position
        
        char[] chars = new char[getMax(lengthes)];
        String[] array = createArray(entries);
        CharBuffer charBuffer = buffer.asCharBuffer();
        int readBytes = 0;
        for (int i = 0; i < entries; ++i) {
            assert (lengthes[i] >= -1);
            if (lengthes[i] == -1) continue; // null
            readBytes += lengthes[i] * 2; // always 2 bytes assuming (kind of) UTF-16
            charBuffer.get(chars, 0, lengthes[i]);
            array[i] = String.valueOf(chars, 0, lengthes[i]);
        }
        buffer.position(buffer.position() + readBytes); // advance original buffer position
        return array;
    }
    @Override
    public int deserializeArray(ByteBuffer buffer, String[] array)
    		throws IOException, ArrayIndexOutOfBoundsException {
        int entries = buffer.getInt();
        assert (entries >= -1);
        if (entries == -1) return 0;
        if (entries > array.length) {
        	throw new ArrayIndexOutOfBoundsException ("array capacity=" + array.length + ", but data length=" + entries);
        }

        IntBuffer lengthBuffer = buffer.asIntBuffer();
        int[] lengthes = new int[entries];
        lengthBuffer.get(lengthes);
        buffer.position(buffer.position() + entries * 4); // advance original buffer position

        char[] chars = new char[getMax(lengthes)];
        CharBuffer charBuffer = buffer.asCharBuffer();
        int readBytes = 0;
        for (int i = 0; i < entries; ++i) {
            assert (lengthes[i] >= -1);
            if (lengthes[i] == -1) continue; // null
            readBytes += lengthes[i] * 2; // always 2 bytes assuming (kind of) UTF-16
            charBuffer.get(chars, 0, lengthes[i]);
            array[i] = String.valueOf(chars, 0, lengthes[i]);
        }
        buffer.position(buffer.position() + readBytes); // advance original buffer position
        return entries;
    }
    @Override
    public int serializeArray(String[] array, ByteBuffer buffer) {
        // see the above function comment
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        int[] lengthes = new int[array.length];
        for (int i = 0; i < array.length; ++i) {
            lengthes[i] = (array[i] == null ? -1 : array[i].length());
        }

        IntBuffer lengthBuffer = buffer.asIntBuffer();
        lengthBuffer.put(lengthes);
        buffer.position(buffer.position() + array.length * 4); // advance original buffer position

        CharBuffer charBuffer = buffer.asCharBuffer();
        int writtenChars = 0;
        for (int i = 0; i < array.length; ++i) {
            if (array[i] == null) continue;
            charBuffer.put (array[i]);
            writtenChars += array[i].length();
        }
        buffer.position(buffer.position() + writtenChars * 2); // advance original buffer position
        return (array.length + 1) * 4 + writtenChars * 2;
    }
    @Override
    public int getSerializedByteSize(String[] array) {
        if (array == null) return 4;
        int writtenBytes = (array.length + 1) * 4;
        for (int i = 0; i < array.length; ++i) {
            if (array[i] != null) {
                writtenBytes += array[i].length() * 2;
            }
        }
        return writtenBytes;
    }

    
    @Override
    public String[] mergeDictionary(String[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        String[] tmpDictionary = createArray(arrays[0].length * 2);
        int curCount = 0;
        int[] positions = new int[dictionaries];
        int[] sizes = new int[dictionaries];
        int finishedDictionaryCount = 0;

        for (int i = 0; i < dictionaries; ++i) {
            sizes[i] = arrays[i].length;
            conversions[i] = new int[sizes[i]];
        }

        while (finishedDictionaryCount < dictionaries) {
            boolean picked = false;
            String minVal = null;
            for (int i = 0; i < dictionaries; ++i) {
                if (positions[i] == sizes[i]) {
                    continue;
                }
                if (!picked || arrays[i][positions[i]].compareTo(minVal) < 0) {
                    picked = true;
                    minVal = arrays[i][positions[i]];
                }
            }
            assert (picked);

            for (int i = 0; i < dictionaries; ++i) {
                if (positions[i] == sizes[i] || !arrays[i][positions[i]].equals(minVal)) {
                    continue;
                }
                conversions[i][positions[i]] = curCount;
                ++positions[i];
                if (positions[i] == sizes[i]) {
                    ++finishedDictionaryCount;
                }
            }
            tmpDictionary[curCount] = minVal;
            ++curCount;
            
            if (tmpDictionary.length == curCount) {
                String[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        String[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
    
    @Override
    public String minValue() {
    	return "";
    }
    @Override
    public String maxValue() {
    	// no way to represent 'max' value
    	return null;
    }
}