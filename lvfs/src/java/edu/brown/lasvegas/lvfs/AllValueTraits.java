package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Defines all value traits classes.
 * Didn't like tens of traits classes defined in each file.. 
 */
public final class AllValueTraits {
    
    // traits for fixed-len number types.
    // Array type is primitive (eg int[]), not Integer[], for performance.
    // This means that these types are essentially NOT NULL.
    public static TinyintValueTraits getTraits (Byte v) {
        return new TinyintValueTraits();
    }
    public static final class TinyintValueTraits extends FixLenValueTraits<Byte, byte[]>{
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
    }

    public static final class SmallintValueTraits extends FixLenValueTraits<Short, short[]> {
        @Override
        public Short readValue(RawValueReader reader) throws IOException {
            return reader.readShort();
        }
        @Override
        public int readValues(RawValueReader reader, short[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asShortBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public void writeValue(RawValueWriter writer, Short value) throws IOException {
            writer.writeShort(value);
        }
        @Override
        public void writeValues(RawValueWriter writer, short[] values, int off, int len) throws IOException {
            int reserved = reserveConversionBufferSize(len);
            ByteBuffer.wrap(conversionBuffer).asShortBuffer().put(values, off, len);
            writer.writeBytes(conversionBuffer, 0, reserved);
        }
        @Override
        public short getBitsPerValue() {
            return 16;
        }
    }

    public static final class IntegerValueTraits extends FixLenValueTraits<Integer, int[]> {
        @Override
        public Integer readValue(RawValueReader reader) throws IOException {
            return reader.readInt();
        }

        @Override
        public int readValues(RawValueReader reader, int[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asIntBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public void writeValue(RawValueWriter writer, Integer value) throws IOException {
            writer.writeInt(value);
        }
        @Override
        public void writeValues(RawValueWriter writer, int[] values, int off, int len) throws IOException {
            int reserved = reserveConversionBufferSize(len);
            ByteBuffer.wrap(conversionBuffer).asIntBuffer().put(values, off, len);
            writer.writeBytes(conversionBuffer, 0, reserved);
        }
        @Override
        public short getBitsPerValue() {
            return 32;
        }
    }

    public static final class BigintValueTraits extends FixLenValueTraits<Long, long[]> {
        @Override
        public Long readValue(RawValueReader reader) throws IOException {
            return reader.readLong();
        }
        @Override
        public int readValues(RawValueReader reader, long[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asLongBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public void writeValue(RawValueWriter writer, Long value) throws IOException {
            writer.writeLong(value);
        }
        @Override
        public void writeValues(RawValueWriter writer, long[] values, int off, int len) throws IOException {
            int reserved = reserveConversionBufferSize(len);
            ByteBuffer.wrap(conversionBuffer).asLongBuffer().put(values, off, len);
            writer.writeBytes(conversionBuffer, 0, reserved);
        }
        @Override
        public short getBitsPerValue() {
            return 64;
        }
    }
    
    public static final class FloatValueTraits extends FixLenValueTraits<Float, float[]> {
        @Override
        public Float readValue(RawValueReader reader) throws IOException {
            return reader.readFloat();
        }
        @Override
        public int readValues(RawValueReader reader, float[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asFloatBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public void writeValue(RawValueWriter writer, Float value) throws IOException {
            writer.writeFloat(value);
        }
        @Override
        public void writeValues(RawValueWriter writer, float[] values, int off, int len) throws IOException {
            int reserved = reserveConversionBufferSize(len);
            ByteBuffer.wrap(conversionBuffer).asFloatBuffer().put(values, off, len);
            writer.writeBytes(conversionBuffer, 0, reserved);
        }
        @Override
        public short getBitsPerValue() {
            return 32;
        }
    }

    public static final class DoubleValueTraits extends FixLenValueTraits<Double, double[]> {
        @Override
        public Double readValue(RawValueReader reader) throws IOException {
            return reader.readDouble();
        }
        @Override
        public int readValues(RawValueReader reader, double[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asDoubleBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public void writeValue(RawValueWriter writer, Double value) throws IOException {
            writer.writeDouble(value);
        }
        @Override
        public void writeValues(RawValueWriter writer, double[] values, int off, int len) throws IOException {
            int reserved = reserveConversionBufferSize(len);
            ByteBuffer.wrap(conversionBuffer).asDoubleBuffer().put(values, off, len);
            writer.writeBytes(conversionBuffer, 0, reserved);
        }
        @Override
        public short getBitsPerValue() {
            return 64;
        }
    }

    // traits for variable-len types.
    // these are simpler in terms of traits, but not as fast as fixed-len types.

    /**
     * Traits for variable-length char (java-String).
     */
    public static final class VarcharValueTraits extends VarLenValueTraits<String> {
        @Override
        public String readValue(RawValueReader reader) throws IOException {
            return reader.readStringWithLengthHeader();
        }
        @Override
        public void writeValue(RawValueWriter writer, String value) throws IOException {
            writer.writeBytesWithLengthHeader(value.getBytes(RawValueWriter.CHARSET));
        }
    }

    /**
     * Traits for variable-length binary data (java-byte[]).
     */
    public static final class VarbinValueTraits extends VarLenValueTraits<byte[]> {
        @Override
        public byte[] readValue(RawValueReader reader) throws IOException {
            return reader.readBytesWithLengthHeader();
        }
        @Override
        public void writeValue(RawValueWriter writer, byte[] value) throws IOException {
            writer.writeBytesWithLengthHeader(value);
        }
    }

    private AllValueTraits() {}
}
