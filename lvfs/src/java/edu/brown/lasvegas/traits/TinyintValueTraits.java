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

/** Traits for TINYINT (java Byte/byte[]). BOOLEAN also falls into this (internally). */
public final class TinyintValueTraits implements FixLenValueTraits<Byte, byte[]>{
    @Override
    public Byte readValue(RawValueReader reader) throws IOException {
        return reader.readByte();
    }
    @Override
    public int readValues(RawValueReader reader, byte[] buffer, int off, int len) throws IOException {
        return reader.readBytes(buffer, off, len);
    }
    @Override
    public void writeValue(RawValueWriter writer, Byte value) throws IOException {
        writer.writeByte(value);
    }
    @Override
    public void writeValues(RawValueWriter writer, byte[] values, int off, int len) throws IOException {
        writer.writeBytes(values, off, len);
    }
    @Override
    public short getBitsPerValue() {
        return 8;
    }
    // these utilize the fact that AT is a primitive array
    @Override
    public void writeRunLengthes(TypedRLEWriter<Byte, byte[]> writer, byte[] values, int off, int len) throws IOException {
        ValueRun<Byte> cur = writer.getCurrentRun();
        byte curValue = cur.value;
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
    public byte[] createArray(int size) {
        return new byte[size];
    }
    @Override
    public int length(byte[] array) {
        return array.length;
    }
    @Override
    public byte[] toArray(Collection<Byte> values) {
        final int length = values.size();
        byte[] array = createArray(length);
        Iterator<Byte> it = values.iterator();
        for (int i = 0; i < length; ++i) {
            array[i] = it.next();
        }
        return array;
    }
    @Override
    public int binarySearch(byte[] array, Byte value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(byte[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(byte[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(byte[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(byte[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    @Override
    public byte[] reorder(byte[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        byte[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    @Override
    public int countDistinct(byte[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        byte prev = array[0];
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
    public void fillArray(Byte value, byte[] array, int off, int len) {
        Arrays.fill(array, off, off + len, value);
    }
    @Override
    public Byte get(byte[] array, int index) {
        return array[index];
    }
    @Override
    public void set(byte[] array, int index, Byte value) {
        array [index] = value;
    }
    @Override
    public byte[] deserializeArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length == -1) return null;
        byte[] array = createArray(length);
        buffer.get(array);
        return array;
    }
    @Override
    public int serializeArray(byte[] array, ByteBuffer buffer) {
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        buffer.put(array);
        return 4 + array.length;
    }
    @Override
    public int getSerializedByteSize(byte[] array) {
        if (array == null) return 4;
        return 4 + array.length;
    }
    
    @Override
    public byte[] mergeDictionary(byte[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        byte[] tmpDictionary = createArray(arrays[0].length * 2);
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
            byte minVal = 0;
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
                byte[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        byte[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
}