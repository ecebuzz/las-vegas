package edu.brown.lasvegas.traits;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collection;

import edu.brown.lasvegas.lvfs.RawValueReader;
import edu.brown.lasvegas.lvfs.RawValueWriter;
import edu.brown.lasvegas.lvfs.TypedRLEWriter;
import edu.brown.lasvegas.lvfs.ValueRun;
import edu.brown.lasvegas.util.ByteArray;
import edu.brown.lasvegas.util.KeyValueArrays;

/**
 * Traits for variable-length binary data (ByteArray).
 */
public final class VarbinValueTraits implements VarLenValueTraits<ByteArray> {
    @Override
    public ByteArray readValue(RawValueReader reader) throws IOException {
        return new ByteArray(reader.readBytesWithLengthHeader());
    }
    @Override
    public void writeValue(RawValueWriter writer, ByteArray value) throws IOException {
        writer.writeBytesWithLengthHeader(value.getBytes());
    }
    @Override
    public void writeRunLengthes(TypedRLEWriter<ByteArray, ByteArray[]> writer, ByteArray[] values, int off, int len) throws IOException {
        ValueRun<ByteArray> cur = writer.getCurrentRun();
        ByteArray curValue = cur.value;
        for (int i = off; i < off + len; ++i) {
            // notice that it's not Object#equals() but Arrays.equals()
            // otherwise it's a pointer comparison!!
            if (values[i].equals(curValue)) {
                ++cur.runLength;
            } else {
                cur = writer.startNewRun(values[i], 1);
                curValue = values[i];
            }
        }
    }
    @Override
    public ByteArray[] createArray(int size) {
        return new ByteArray[size];
    }
    @Override
    public ByteArray[][] create2DArray(int size) {
        return new ByteArray[size][];
    }
    @Override
    public int length(ByteArray[] array) {
        return array.length;
    }
    @Override
    public ByteArray[] toArray(Collection<ByteArray> values) {
        return values.toArray(createArray(values.size()));
    }
    @Override
    public int binarySearch(ByteArray[] array, ByteArray value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(ByteArray[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(ByteArray[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(ByteArray[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(ByteArray[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    @Override
    public ByteArray[] reorder(ByteArray[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        ByteArray[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    @Override
    public int countDistinct(ByteArray[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        ByteArray prev = array[0];
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
    public void fillArray(ByteArray value, ByteArray[] array, int off, int len) {
        // only this object has to do clone() because byte[] is mutable.
        // all the other objects work with immutable objects or primitives, so no worry.
        for (int i = off; i < off + len; ++i) {
            array[i] = new ByteArray(value);
        }
    }
    @Override
    public ByteArray get(ByteArray[] array, int index) {
        return array[index];
    }
    @Override
    public void set(ByteArray[] array, int index, ByteArray value) {
        array[index] = value;
    }
    @Override
    public ByteArray[] deserializeArray(ByteBuffer buffer) {
        // see VarcharValueTraits#deserializeArray() for data format.
        // the only difference is that the data part is byte[], not char[].
        int entries = buffer.getInt();
        assert (entries >= -1);
        if (entries == -1) return null;

        IntBuffer lengthBuffer = buffer.asIntBuffer();
        int[] lengthes = new int[entries];
        lengthBuffer.get(lengthes);
        buffer.position(buffer.position() + entries * 4); // advance original buffer position

        // we use the original byte buffer below, so no need to re-position ourselves
        ByteArray[] array = createArray(entries);
        for (int i = 0; i < entries; ++i) {
            assert (lengthes[i] >= -1);
            if (lengthes[i] == -1) continue; // null
            byte[] data = new byte[lengthes[i]];
            buffer.get(data);
            array[i] = new ByteArray(data);
        }
        return array;
    }
    @Override
    public int serializeArray(ByteArray[] array, ByteBuffer buffer) {
        // see the above function comment
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        int[] lengthes = new int[array.length];
        lengthes[0] = array.length;
        for (int i = 0; i < array.length; ++i) {
            lengthes[i] = (array[i] == null || array[i].getBytes() == null ? -1 :  array[i].getBytes().length);
        }

        IntBuffer lengthBuffer = buffer.asIntBuffer();
        lengthBuffer.put(lengthes);
        buffer.position(buffer.position() + array.length * 4); // advance original buffer position

        // we use the original byte buffer below, so no need to re-position ourselves
        int writtenBytes = (array.length + 1) * 4;
        for (int i = 0; i < array.length; ++i) {
            if (array[i] == null || array[i].getBytes() == null) continue;
            buffer.put (array[i].getBytes());
            writtenBytes += array[i].getBytes().length;
        }
        return writtenBytes;
    }
    @Override
    public int getSerializedByteSize(ByteArray[] array) {
        if (array == null) return 4;
        int writtenBytes = (array.length + 1) * 4;
        for (int i = 0; i < array.length; ++i) {
            if (array[i] != null &&  array[i].getBytes() != null) {
                writtenBytes += array[i].getBytes().length;
            }
        }
        return writtenBytes;
    }

    @Override
    public ByteArray[] mergeDictionary(ByteArray[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        ByteArray[] tmpDictionary = createArray(arrays[0].length * 2);
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
            ByteArray minVal = null;
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
                ByteArray[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        ByteArray[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
    
    @Override
    public ByteArray minValue() {
    	return new ByteArray(new byte[0]);
    }
    @Override
    public ByteArray maxValue() {
    	// no way to represent 'max' value
    	return null;
    }
}