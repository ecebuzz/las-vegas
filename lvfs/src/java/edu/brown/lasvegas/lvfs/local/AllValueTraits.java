package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Defines all value traits classes.
 * Didn't like tens of traits classes defined in each file.. 
 */
public final class AllValueTraits {
    
    // traits for fixed-len numfer types.
    // Array type is primitive (eg int[]), not Integer[], for performance.
    // This means that these types are essentially NOT NULL.

    public static class TinyintValueTraits extends FixLenValueTraits<Byte, byte[]>{
        @Override
        public Byte readValue(LocalRawFileReader reader) throws IOException {
            return reader.readByte();
        }
        @Override
        public int readValues(LocalRawFileReader reader, byte[] buffer, int off, int len) throws IOException {
            return reader.readBytes(buffer, off, len);
        }
        @Override
        public short getBitsPerValue() {
            return 8;
        }
    }

    public static class SmallintValueTraits extends FixLenValueTraits<Short, short[]> {
        @Override
        public Short readValue(LocalRawFileReader reader) throws IOException {
            return reader.readShort();
        }
        @Override
        public int readValues(LocalRawFileReader reader, short[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asShortBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public short getBitsPerValue() {
            return 16;
        }
    }

    public static class IntegerValueTraits extends FixLenValueTraits<Integer, int[]> {
        @Override
        public Integer readValue(LocalRawFileReader reader) throws IOException {
            return reader.readInt();
        }

        @Override
        public int readValues(LocalRawFileReader reader, int[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asIntBuffer().get(buffer, off, len);
            return len;
        }
        
        @Override
        public short getBitsPerValue() {
            return 32;
        }
    }

    public static class BigintValueTraits extends FixLenValueTraits<Long, long[]> {
        @Override
        public Long readValue(LocalRawFileReader reader) throws IOException {
            return reader.readLong();
        }
        @Override
        public int readValues(LocalRawFileReader reader, long[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asLongBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public short getBitsPerValue() {
            return 64;
        }
    }
    
    public static class FloatValueTraits extends FixLenValueTraits<Float, float[]> {
        @Override
        public Float readValue(LocalRawFileReader reader) throws IOException {
            return reader.readFloat();
        }
        @Override
        public int readValues(LocalRawFileReader reader, float[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asFloatBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public short getBitsPerValue() {
            return 32;
        }
    }

    public static class DoubleValueTraits extends FixLenValueTraits<Double, double[]> {
        @Override
        public Double readValue(LocalRawFileReader reader) throws IOException {
            return reader.readDouble();
        }
        @Override
        public int readValues(LocalRawFileReader reader, double[] buffer, int off, int len) throws IOException {
            len = readIntoConversionBuffer(reader, len);
            ByteBuffer.wrap(conversionBuffer).asDoubleBuffer().get(buffer, off, len);
            return len;
        }
        @Override
        public short getBitsPerValue() {
            return 64;
        }
    }

    /**
     * Traits for variable-length char (java-String).
     */
    public static class VarcharValueTraits extends VarLenValueTraits<String> {
        /** internal buffer to tentatively hold byte array. */
        private byte[] conversionBuffer = new byte[1024];

        @Override
        public String readValue(LocalRawFileReader reader, int length) throws IOException {
            if (length > conversionBuffer.length) {
                conversionBuffer = new byte[length];
            }
            int read = reader.readBytes(conversionBuffer, 0, length);
            return new String(conversionBuffer, 0, read, CHARSET);
        }
        
        private static Charset CHARSET;
        static {
            try {
                CHARSET = Charset.forName("UTF-8");
            } catch (Exception ex) {
                CHARSET = Charset.defaultCharset();
            }
        }
    }

    /**
     * Traits for variable-length binary data (java-byte[]).
     */
    public static class VarbinValueTraits extends VarLenValueTraits<byte[]> {
        @Override
        public byte[] readValue(LocalRawFileReader reader, int length) throws IOException {
            // this trait directly returns the byte array. so, no buffering is possible.
            byte[] buffer = new byte[length];
            int read = reader.readBytes(buffer, 0, length);
            assert (read == length);
            return buffer;
        }
    }

    private AllValueTraits() {}
}
