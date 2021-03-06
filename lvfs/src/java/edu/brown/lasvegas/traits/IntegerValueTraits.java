package edu.brown.lasvegas.traits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import edu.brown.lasvegas.lvfs.RawValueReader;
import edu.brown.lasvegas.lvfs.RawValueWriter;
import edu.brown.lasvegas.lvfs.TypedRLEWriter;
import edu.brown.lasvegas.lvfs.ValueRun;
import edu.brown.lasvegas.util.KeyValueArrays;

/** Traits for INTEGER (java Integer/int[]). */
public final class IntegerValueTraits implements FixLenValueTraits<Integer, int[]> {
    @Override
    public Integer readValue(RawValueReader reader) throws IOException {
        return reader.readInt();
    }

    @Override
    public int readValues(RawValueReader reader, int[] buffer, int off, int len) throws IOException {
        return reader.readInts(buffer, off, len);
    }
    @Override
    public void writeValue(RawValueWriter writer, Integer value) throws IOException {
        writer.writeInt(value);
    }
    @Override
    public void writeValues(RawValueWriter writer, int[] values, int off, int len) throws IOException {
        writer.writeInts(values, off, len);
    }
    @Override
    public short getBitsPerValue() {
        return 32;
    }
    @Override
    public void writeRunLengthes(TypedRLEWriter<Integer, int[]> writer, int[] values, int off, int len) throws IOException {
        ValueRun<Integer> cur = writer.getCurrentRun();
        int curValue = cur.value;
        for (int i = off; i < off + len; ++i) {
            if (values[i] == curValue) {
                ++cur.runLength;
            } else {
                cur = writer.startNewRun(values[i], 1);
                curValue = values[i];
            }
        }
    }
    @Override
    public int[] createArray(int size) {
        return new int[size];
    }
    @Override
    public int[][] create2DArray(int size) {
        return new int[size][];
    }
    @Override
    public int length(int[] array) {
        return array.length;
    }
    @Override
    public int[] toArray(Collection<Integer> values) {
        final int length = values.size();
        int[] array = createArray(length);
        Iterator<Integer> it = values.iterator();
        for (int i = 0; i < length; ++i) {
            array[i] = it.next();
        }
        return array;
    }
    @Override
    public int binarySearch(int[] array, Integer value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(int[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(int[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(int[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(int[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    @Override
    public int[] reorder(int[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        int[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    @Override
    public int countDistinct(int[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        int prev = array[0];
        int distinctValues = 1;
        for (int i = 1; i < len; ++i) {
            if (array[i] != prev) {
                ++distinctValues;
                prev = array[i];
            }
        }
        return distinctValues;
    }
    @Override
    public void fillArray(Integer value, int[] array, int off, int len) {
        Arrays.fill(array, off, off + len, value);
    }
    @Override
    public Integer get(int[] array, int index) {
        return array[index];
    }
    @Override
    public void set(int[] array, int index, Integer value) {
        array[index] = value;
    }
    @Override
    public int[] deserializeArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length == -1) return null;
        int[] array = createArray(length);
        buffer.asIntBuffer().get(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return array;
    }
    @Override
    public int deserializeArray(ByteBuffer buffer, int[] array)
    		throws IOException, ArrayIndexOutOfBoundsException {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length > array.length) {
        	throw new ArrayIndexOutOfBoundsException ("array capacity=" + array.length + ", but data length=" + length);
        }
        if (length == -1) return 0;
        buffer.asIntBuffer().get(array, 0, length); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + length * (getBitsPerValue() / 8)); // so, advance it here
        return length;
    }
    @Override
    public int serializeArray(int[] array, ByteBuffer buffer) {
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        buffer.asIntBuffer().put(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return 4 + array.length * (getBitsPerValue() / 8);
    }
    @Override
    public int getSerializedByteSize(int[] array) {
        if (array == null) return 4;
        return 4 + array.length * (getBitsPerValue() / 8);
    }

    
    @Override
    public int[] mergeDictionary(int[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        int[] tmpDictionary = createArray(arrays[0].length * 2);
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
            int minVal = 0;
            for (int i = 0; i < dictionaries; ++i) {
                if (positions[i] == sizes[i]) {
                    continue;
                }
                if (!picked || arrays[i][positions[i]] < minVal) {
                    picked = true;
                    minVal = arrays[i][positions[i]];
                }
            }
            assert (picked);

            for (int i = 0; i < dictionaries; ++i) {
                if (positions[i] == sizes[i] || arrays[i][positions[i]] != minVal) {
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
                int[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        int[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
    @Override
    public Integer minValue() {
    	return Integer.MIN_VALUE;
    }
    @Override
    public Integer maxValue() {
    	return Integer.MAX_VALUE;
    }
}