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

/** Traits for SMALLINT (java Short/short[]). */
public final class SmallintValueTraits implements FixLenValueTraits<Short, short[]> {
    @Override
    public Short readValue(RawValueReader reader) throws IOException {
        return reader.readShort();
    }
    @Override
    public int readValues(RawValueReader reader, short[] buffer, int off, int len) throws IOException {
        return reader.readShorts(buffer, off, len);
    }
    @Override
    public void writeValue(RawValueWriter writer, Short value) throws IOException {
        writer.writeShort(value);
    }
    @Override
    public void writeValues(RawValueWriter writer, short[] values, int off, int len) throws IOException {
        writer.writeShorts(values, off, len);
    }
    @Override
    public short getBitsPerValue() {
        return 16;
    }
    @Override
    public void writeRunLengthes(TypedRLEWriter<Short, short[]> writer, short[] values, int off, int len) throws IOException {
        ValueRun<Short> cur = writer.getCurrentRun();
        short curValue = cur.value;
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
    public short[] createArray(int size) {
        return new short[size];
    }
    @Override
    public short[][] create2DArray(int size) {
        return new short[size][];
    }
    @Override
    public int length(short[] array) {
        return array.length;
    }
    @Override
    public short[] toArray(Collection<Short> values) {
        final int length = values.size();
        short[] array = createArray(length);
        Iterator<Short> it = values.iterator();
        for (int i = 0; i < length; ++i) {
            array[i] = it.next();
        }
        return array;
    }
    @Override
    public int binarySearch(short[] array, Short value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(short[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(short[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(short[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(short[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    @Override
    public short[] reorder(short[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        short[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    @Override
    public int countDistinct(short[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        short prev = array[0];
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
    public void fillArray(Short value, short[] array, int off, int len) {
        Arrays.fill(array, off, off + len, value);
    }
    @Override
    public Short get(short[] array, int index) {
        return array[index];
    }
    @Override
    public void set(short[] array, int index, Short value) {
        array[index] = value;
    }
    @Override
    public short[] deserializeArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length == -1) return null;
        short[] array = createArray(length);
        buffer.asShortBuffer().get(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return array;
    }
    @Override
    public int serializeArray(short[] array, ByteBuffer buffer) {
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        buffer.asShortBuffer().put(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return 4 + array.length * (getBitsPerValue() / 8);
    }
    @Override
    public int getSerializedByteSize(short[] array) {
        if (array == null) return 4;
        return 4 + array.length * (getBitsPerValue() / 8);
    }

    
    @Override
    public short[] mergeDictionary(short[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        short[] tmpDictionary = createArray(arrays[0].length * 2);
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
            short minVal = 0;
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
                short[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        short[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
    
    @Override
    public Short minValue() {
    	return Short.MIN_VALUE;
    }
    @Override
    public Short maxValue() {
    	return Short.MAX_VALUE;
    }
}