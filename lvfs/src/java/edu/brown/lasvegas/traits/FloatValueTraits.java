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

/** Traits for FLOAT (java Float/float[]). */
public final class FloatValueTraits implements FixLenValueTraits<Float, float[]> {
    @Override
    public Float readValue(RawValueReader reader) throws IOException {
        return reader.readFloat();
    }
    @Override
    public int readValues(RawValueReader reader, float[] buffer, int off, int len) throws IOException {
        return reader.readFloats(buffer, off, len);
    }
    @Override
    public void writeValue(RawValueWriter writer, Float value) throws IOException {
        writer.writeFloat(value);
    }
    @Override
    public void writeValues(RawValueWriter writer, float[] values, int off, int len) throws IOException {
        writer.writeFloats(values, off, len);
    }
    @Override
    public short getBitsPerValue() {
        return 32;
    }
    @Override
    public void writeRunLengthes(TypedRLEWriter<Float, float[]> writer, float[] values, int off, int len) throws IOException {
        ValueRun<Float> cur = writer.getCurrentRun();
        float curValue = cur.value;
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
    public float[] createArray(int size) {
        return new float[size];
    }
    @Override
    public float[][] create2DArray(int size) {
        return new float[size][];
    }
    @Override
    public int length(float[] array) {
        return array.length;
    }
    @Override
    public float[] toArray(Collection<Float> values) {
        final int length = values.size();
        float[] array = createArray(length);
        Iterator<Float> it = values.iterator();
        for (int i = 0; i < length; ++i) {
            array[i] = it.next();
        }
        return array;
    }
    @Override
    public int binarySearch(float[] array, Float value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(float[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(float[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(float[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(float[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    @Override
    public float[] reorder(float[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        float[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    /** this doesn't correctly count NaN, and positive vs negative zero. */
    @Override
    public int countDistinct(float[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        float prev = array[0];
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
    public void fillArray(Float value, float[] array, int off, int len) {
        Arrays.fill(array, off, off + len, value);
    }
    @Override
    public Float get(float[] array, int index) {
        return array[index];
    }
    @Override
    public void set(float[] array, int index, Float value) {
        array [index] = value;
    }
    @Override
    public float[] deserializeArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length == -1) return null;
        float[] array = createArray(length);
        buffer.asFloatBuffer().get(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return array;
    }
    @Override
    public int deserializeArray(ByteBuffer buffer, float[] array)
    		throws IOException, ArrayIndexOutOfBoundsException {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length > array.length) {
        	throw new ArrayIndexOutOfBoundsException ("array capacity=" + array.length + ", but data length=" + length);
        }
        if (length == -1) return 0;
        buffer.asFloatBuffer().get(array, 0, length); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + length * (getBitsPerValue() / 8)); // so, advance it here
        return length;
    }
    @Override
    public int serializeArray(float[] array, ByteBuffer buffer) {
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        buffer.asFloatBuffer().put(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return 4 + array.length * (getBitsPerValue() / 8);
    }
    @Override
    public int getSerializedByteSize(float[] array) {
        if (array == null) return 4;
        return 4 + array.length * (getBitsPerValue() / 8);
    }

    
    @Override
    public float[] mergeDictionary(float[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        float[] tmpDictionary = createArray(arrays[0].length * 2);
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
            float minVal = 0;
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
                float[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        float[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
    
    @Override
    public Float minValue() {
    	return Float.MIN_VALUE;
    }
    @Override
    public Float maxValue() {
    	return Float.MAX_VALUE;
    }
}