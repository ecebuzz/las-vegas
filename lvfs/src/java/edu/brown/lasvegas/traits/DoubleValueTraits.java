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

/** Traits for DOUBLE (java Double/double[]). */
public final class DoubleValueTraits implements FixLenValueTraits<Double, double[]> {
    @Override
    public Double readValue(RawValueReader reader) throws IOException {
        return reader.readDouble();
    }
    @Override
    public int readValues(RawValueReader reader, double[] buffer, int off, int len) throws IOException {
        return reader.readDoubles(buffer, off, len);
    }
    @Override
    public void writeValue(RawValueWriter writer, Double value) throws IOException {
        writer.writeDouble(value);
    }
    @Override
    public void writeValues(RawValueWriter writer, double[] values, int off, int len) throws IOException {
        writer.writeDoubles(values, off, len);
    }
    @Override
    public short getBitsPerValue() {
        return 64;
    }
    @Override
    public double[] createArray(int size) {
        return new double[size];
    }
    @Override
    public double[][] create2DArray(int size) {
        return new double[size][];
    }
    @Override
    public int length(double[] array) {
        return array.length;
    }
    @Override
    public double[] toArray(Collection<Double> values) {
        final int length = values.size();
        double[] array = createArray(length);
        Iterator<Double> it = values.iterator();
        for (int i = 0; i < length; ++i) {
            array[i] = it.next();
        }
        return array;
    }
    @Override
    public int binarySearch(double[] array, Double value) {
        return Arrays.binarySearch(array, value);
    }
    @Override
    public void sort(double[] keys) {
        sort (keys, 0, keys.length);
    }
    @Override
    public void sort(double[] keys, int fromIndex, int toIndex) {
        Arrays.sort(keys, fromIndex, toIndex);
    }
    @Override
    public void sortKeyValue(double[] keys, int[] values) {
        sortKeyValue (keys, values, 0, keys.length);
    }
    @Override
    public void sortKeyValue(double[] keys, int[] values, int fromIndex, int toIndex) {
        KeyValueArrays.sort(keys, values, fromIndex, toIndex);
    }
    /** this doesn't correctly count NaN, and positive vs negative zero. */
    @Override
    public double[] reorder(double[] src, int[] srcPos) {
        final int len = srcPos.length;
        assert (src.length == len);
        double[] dest = createArray(len);
        for (int i = 0; i < len; ++i) {
            dest[i] = src[srcPos[i]];
        }
        return dest;
    }
    @Override
    public int countDistinct(double[] array) {
        final int len = array.length;
        if (len == 0) return 0;
        double prev = array[0];
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
    public void writeRunLengthes(TypedRLEWriter<Double, double[]> writer, double[] values, int off, int len) throws IOException {
        ValueRun<Double> cur = writer.getCurrentRun();
        double curValue = cur.value;
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
    public void fillArray(Double value, double[] array, int off, int len) {
        Arrays.fill(array, off, off + len, value);
    }
    @Override
    public Double get(double[] array, int index) {
        return array[index];
    }
    @Override
    public void set(double[] array, int index, Double value) {
        array[index] = value;
    }
    @Override
    public double[] deserializeArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        assert (length >= -1);
        if (length == -1) return null;
        double[] array = createArray(length);
        buffer.asDoubleBuffer().get(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return array;
    }
    @Override
    public int serializeArray(double[] array, ByteBuffer buffer) {
        if (array == null) {
            buffer.putInt(-1);
            return 4;
        }
        buffer.putInt(array.length);
        buffer.asDoubleBuffer().put(array); // remember this doesn't advance the original byte buffer's position
        buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
        return 4 + array.length * (getBitsPerValue() / 8);
    }
    @Override
    public int getSerializedByteSize(double[] array) {
        if (array == null) return 4;
        return 4 + array.length * (getBitsPerValue() / 8);
    }

    @Override
    public double[] mergeDictionary(double[][] arrays, int[][] conversions) {
        int dictionaries = arrays.length;
        assert (dictionaries >= 2);
        assert (dictionaries == conversions.length);
        double[] tmpDictionary = createArray(arrays[0].length * 2);
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
            double minVal = 0;
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
                double[] tmpDictionaryExpanded = createArray(tmpDictionary.length * 2);
                System.arraycopy(tmpDictionary, 0, tmpDictionaryExpanded, 0, tmpDictionary.length);
                tmpDictionary = tmpDictionaryExpanded;
            }
        }

        // adjust the array size
        double[] finalDictionary = createArray(curCount);
        System.arraycopy(tmpDictionary, 0, finalDictionary, 0, curCount);
        return finalDictionary;
    }
}