package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.util.Arrays;

/**
 * Defines all value traits classes.
 * Didn't like tens of traits classes defined in each file.. 
 */
public final class AllValueTraits {
    
    // traits for fixed-len number types.
    // Array type is primitive (eg int[]), not Integer[], for performance.
    // This means that these types are essentially NOT NULL.
    public static final class TinyintValueTraits implements FixLenValueTraits<Byte, byte[]>{
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
    }

    public static final class SmallintValueTraits implements FixLenValueTraits<Short, short[]> {
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
    }

    public static final class IntegerValueTraits implements FixLenValueTraits<Integer, int[]> {
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
    }

    public static final class BigintValueTraits implements FixLenValueTraits<Long, long[]> {
        @Override
        public Long readValue(RawValueReader reader) throws IOException {
            return reader.readLong();
        }
        @Override
        public int readValues(RawValueReader reader, long[] buffer, int off, int len) throws IOException {
            return reader.readLongs(buffer, off, len);
        }
        @Override
        public void writeValue(RawValueWriter writer, Long value) throws IOException {
            writer.writeLong(value);
        }
        @Override
        public void writeValues(RawValueWriter writer, long[] values, int off, int len) throws IOException {
            writer.writeLongs(values, off, len);
        }
        @Override
        public short getBitsPerValue() {
            return 64;
        }
        @Override
        public void writeRunLengthes(TypedRLEWriter<Long, long[]> writer, long[] values, int off, int len) throws IOException {
            ValueRun<Long> cur = writer.getCurrentRun();
            long curValue = cur.value;
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
        public void fillArray(Long value, long[] array, int off, int len) {
            Arrays.fill(array, off, off + len, value);
        }
        @Override
        public Long get(long[] array, int index) {
            return array[index];
        }
        @Override
        public void set(long[] array, int index, Long value) {
            array[index] = value;
        }
    }
    
    public static final class FloatValueTraits implements FixLenValueTraits<Float, float[]> {
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
    }

    public static final class DoubleValueTraits implements FixLenValueTraits<Double, double[]> {
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
    }

    // traits for variable-len types.
    // these are simpler in terms of traits, but not as fast as fixed-len types.

    /**
     * Traits for variable-length char (java-String).
     */
    public static final class VarcharValueTraits implements VarLenValueTraits<String> {
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
    }

    /**
     * Traits for variable-length binary data (java-byte[]).
     */
    public static final class VarbinValueTraits implements VarLenValueTraits<byte[]> {
        @Override
        public byte[] readValue(RawValueReader reader) throws IOException {
            return reader.readBytesWithLengthHeader();
        }
        @Override
        public void writeValue(RawValueWriter writer, byte[] value) throws IOException {
            writer.writeBytesWithLengthHeader(value);
        }
        @Override
        public void writeRunLengthes(TypedRLEWriter<byte[], byte[][]> writer, byte[][] values, int off, int len) throws IOException {
            ValueRun<byte[]> cur = writer.getCurrentRun();
            byte[] curValue = cur.value;
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
        public void fillArray(byte[] value, byte[][] array, int off, int len) {
            // only this object has to do clone() because byte[] is mutable.
            // all the other objects work with immutable objects or primitives, so no worry.
            for (int i = off; i < off + len; ++i) {
                array[i] = value.clone();
            }
        }
        @Override
        public byte[] get(byte[][] array, int index) {
            return array[index];
        }
        @Override
        public void set(byte[][] array, int index, byte[] value) {
            array[index] = value;
        }
    }

    private AllValueTraits() {}
}
