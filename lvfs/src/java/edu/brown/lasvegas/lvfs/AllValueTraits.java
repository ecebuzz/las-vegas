package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import edu.brown.lasvegas.ColumnType;

/**
 * Defines all value traits classes.
 * Didn't like tens of traits classes defined in each file.. 
 */
public final class AllValueTraits {
    /**
     * Creates an instance of value traits for the given data type.
     */
    public static ValueTraits<?, ?> getInstance (ColumnType type) {
        switch (type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT:
            return new AllValueTraits.BigintValueTraits();
        case INTEGER:
            return new AllValueTraits.IntegerValueTraits();
        case SMALLINT:
            return new AllValueTraits.SmallintValueTraits();
        case BOOLEAN:
        case TINYINT:
            return new AllValueTraits.TinyintValueTraits();
        case FLOAT:
            return new AllValueTraits.FloatValueTraits();
        case DOUBLE:
            return new AllValueTraits.DoubleValueTraits();
        case VARCHAR:
            return new AllValueTraits.VarcharValueTraits();
        case VARBINARY:
            return new AllValueTraits.VarbinValueTraits();
        default:
            throw new IllegalArgumentException("unexpected type: " + type);
        }
    }
    
    // traits for fixed-len number types.
    // Array type is primitive (eg int[]), not Integer[], for performance.
    // This means that these types are essentially NOT NULL.

    /** Traits for TINYINT (java Byte/byte[]). BOOLEAN also falls into this (internally). */
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
    }

    /** Traits for SMALLINT (java Short/short[]). */
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
        public short[] createArray(int size) {
            return new short[size];
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
    }

    /** Traits for INTEGER (java Integer/int[]). */
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
        public int[] createArray(int size) {
            return new int[size];
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
    }

    /** Traits for BIGINT (java Long/long[]). DATE/TIME/TIMESTAMP also fall into this (internally). */
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
        public long[] createArray(int size) {
            return new long[size];
        }
        @Override
        public int length(long[] array) {
            return array.length;
        }
        @Override
        public long[] toArray(Collection<Long> values) {
            final int length = values.size();
            long[] array = createArray(length);
            Iterator<Long> it = values.iterator();
            for (int i = 0; i < length; ++i) {
                array[i] = it.next();
            }
            return array;
        }
        @Override
        public int binarySearch(long[] array, Long value) {
            return Arrays.binarySearch(array, value);
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
        @Override
        public long[] deserializeArray(ByteBuffer buffer) {
            int length = buffer.getInt();
            assert (length >= -1);
            if (length == -1) return null;
            long[] array = createArray(length);
            buffer.asLongBuffer().get(array); // remember this doesn't advance the original byte buffer's position
            buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
            return array;
        }
        @Override
        public int serializeArray(long[] array, ByteBuffer buffer) {
            if (array == null) {
                buffer.putInt(-1);
                return 4;
            }
            buffer.putInt(array.length);
            buffer.asLongBuffer().put(array); // remember this doesn't advance the original byte buffer's position
            buffer.position(buffer.position() + array.length * (getBitsPerValue() / 8)); // so, advance it here
            return 4 + array.length * (getBitsPerValue() / 8);
        }
        @Override
        public int getSerializedByteSize(long[] array) {
            if (array == null) return 4;
            return 4 + array.length * (getBitsPerValue() / 8);
        }
    }
    
    /** Traits for FLOAT (java Float/float[]). */
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
        public float[] createArray(int size) {
            return new float[size];
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
    }

    /** Traits for DOUBLE (java Double/double[]). */
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
        public double[] createArray(int size) {
            return new double[size];
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
        public String[] createArray(int size) {
            return new String[size];
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

            String[] array = createArray(entries);
            CharBuffer charBuffer = buffer.asCharBuffer();
            int readBytes = 0;
            for (int i = 0; i < entries; ++i) {
                assert (lengthes[i] >= -1);
                if (lengthes[i] == -1) continue; // null
                readBytes += lengthes[i] * 2; // always 2 bytes assuming (kind of) UTF-16
                char[] chars = new char[lengthes[i]];
                charBuffer.get(chars);
                array[i] = String.valueOf(chars);
            }
            buffer.position(buffer.position() + readBytes); // advance original buffer position
            return array;
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
                // notice that it's not Object#equals() but Arrays.equals()
                // otherwise it's a pointer comparison!!
                if (Arrays.equals(values[i], curValue)) {
                    ++cur.runLength;
                } else {
                    cur = writer.startNewRun(values[i], 1);
                    curValue = values[i];
                }
            }
        }
        @Override
        public byte[][] createArray(int size) {
            return new byte[size][];
        }
        @Override
        public int length(byte[][] array) {
            return array.length;
        }
        @Override
        public byte[][] toArray(Collection<byte[]> values) {
            return values.toArray(createArray(values.size()));
        }
        @Override
        public int binarySearch(byte[][] array, byte[] value) {
            throw new UnsupportedOperationException("sorting/searching for VARBIN is not supported");
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
        @Override
        public byte[][] deserializeArray(ByteBuffer buffer) {
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
            byte[][] array = createArray(entries);
            for (int i = 0; i < entries; ++i) {
                assert (lengthes[i] >= -1);
                if (lengthes[i] == -1) continue; // null
                byte[] data = new byte[lengthes[i]];
                buffer.get(data);
                array[i] = data;
            }
            return array;
        }
        @Override
        public int serializeArray(byte[][] array, ByteBuffer buffer) {
            // see the above function comment
            if (array == null) {
                buffer.putInt(-1);
                return 4;
            }
            buffer.putInt(array.length);
            int[] lengthes = new int[array.length];
            lengthes[0] = array.length;
            for (int i = 0; i < array.length; ++i) {
                lengthes[i] = (array[i] == null ? -1 : array[i].length);
            }

            IntBuffer lengthBuffer = buffer.asIntBuffer();
            lengthBuffer.put(lengthes);
            buffer.position(buffer.position() + array.length * 4); // advance original buffer position

            // we use the original byte buffer below, so no need to re-position ourselves
            int writtenBytes = (array.length + 1) * 4;
            for (int i = 0; i < array.length; ++i) {
                if (array[i] == null) continue;
                buffer.put (array[i]);
                writtenBytes += array[i].length;
            }
            return writtenBytes;
        }
        @Override
        public int getSerializedByteSize(byte[][] array) {
            if (array == null) return 4;
            int writtenBytes = (array.length + 1) * 4;
            for (int i = 0; i < array.length; ++i) {
                if (array[i] != null) {
                    writtenBytes += array[i].length;
                }
            }
            return writtenBytes;
        }
    }

    private AllValueTraits() {}
}
