package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Interface to receive raw values (no notion of tuples here) to write out.
 * The implementation class defines where to write it out.
 */
public abstract class RawValueWriter {
    private final byte[] smallBuf = new byte[8];

    /** Writes arbitrary byte array. */
    public abstract void writeBytes (byte[] buf, int off, int len) throws IOException;

    /** Writes 1 byte. */
    public final void writeByte (byte v) throws IOException {
        smallBuf[0] = v;
        writeBytes (smallBuf, 0, 1);
    }

    /** Writes 1 byte  (so far we don't compress 8 booleans into 1 byte) as boolean. */
    public final void writeBoolean (boolean v) throws IOException {
        writeByte(v ? (byte) 1 : (byte) 0);
    }

    /** Writes 2 bytes as short. */
    public final void writeShort (short v) throws IOException {
        smallBuf[0] = (byte) (v >>> 8);
        smallBuf[1] = (byte) (v >>> 0);
        writeBytes (smallBuf, 0, 2);
    }
    
    /** Writes 4 bytes as int. */
    public final void writeInt (int v) throws IOException {
        smallBuf[0] = (byte) (v >>> 24);
        smallBuf[1] = (byte) (v >>> 16);
        smallBuf[2] = (byte) (v >>> 8);
        smallBuf[3] = (byte) (v >>> 0);
        writeBytes (smallBuf, 0, 4);
    }
    
    /** Writes 8 bytes as long. */
    public final void writeLong (long v) throws IOException {
        smallBuf[0] = (byte) (v >>> 56);
        smallBuf[1] = (byte) (v >>> 48);
        smallBuf[2] = (byte) (v >>> 40);
        smallBuf[3] = (byte) (v >>> 32);
        smallBuf[4] = (byte) (v >>> 24);
        smallBuf[5] = (byte) (v >>> 16);
        smallBuf[6] = (byte) (v >>> 8);
        smallBuf[7] = (byte) (v >>> 0);
        writeBytes (smallBuf, 0, 8);
    }

    /** Writes 4 bytes as float. */
    public final void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    /** Writes 8 bytes as double. */
    public final void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    /** Write a variable-length String with length header. */
    public final void writeStringWithLengthHeader (String v) throws IOException {
        byte[] bytes = v.getBytes(CHARSET);
        writeBytesWithLengthHeader (bytes);
    }
    /** Write a variable-length byte[] with length header. */
    public final void writeBytesWithLengthHeader (byte[] bytes) throws IOException {
        if (bytes.length < (1 << 7)) {
            // 1 byte length header
            writeByte((byte) 1);
            writeByte((byte) bytes.length);
        } else if (bytes.length < (1 << 15)) {
            // 2 byte length header
            writeByte((byte) 2);
            writeShort((short) bytes.length);
        } else if (bytes.length < (1 << 31)) {
            // 4 byte length header
            writeByte((byte) 4);
            writeInt(bytes.length);
        } else {
            // 8 byte length header (this is not quite implemented as byte[1<<32] isn't possible)
            writeByte((byte) 8);
            writeLong(bytes.length);
        }
        writeBytes(bytes, 0, bytes.length);
    }

    /** temporary buffer to do batch conversion. */
    private byte[] conversionBuffer = new byte[1024];
    private void reserveConversionBufferSize(int bytesToReserve) {
        if (bytesToReserve > conversionBuffer.length) {
            conversionBuffer = new byte[bytesToReserve];
        }
    }

    /** Writes arbitrary number of 2-byte integers at once. */
    public void writeShorts(short[] values, int off, int len) throws IOException {
        reserveConversionBufferSize(len * 2);
        ByteBuffer.wrap(conversionBuffer).asShortBuffer().put(values, off, len);
        writeBytes(conversionBuffer, 0, len * 2);
    }
    /** Writes arbitrary number of 4-byte integers at once. */
    public void writeInts(int[] values, int off, int len) throws IOException {
        reserveConversionBufferSize(len * 4);
        ByteBuffer.wrap(conversionBuffer).asIntBuffer().put(values, off, len);
        writeBytes(conversionBuffer, 0, len * 4);
    }
    /** Writes arbitrary number of 8-byte integers at once. */
    public void writeLongs(long[] values, int off, int len) throws IOException {
        reserveConversionBufferSize(len * 8);
        ByteBuffer.wrap(conversionBuffer).asLongBuffer().put(values, off, len);
        writeBytes(conversionBuffer, 0, len * 8);
    }
    /** Writes arbitrary number of 4-byte floats at once. */
    public void writeFloats(float[] values, int off, int len) throws IOException {
        reserveConversionBufferSize(len * 4);
        ByteBuffer.wrap(conversionBuffer).asFloatBuffer().put(values, off, len);
        writeBytes(conversionBuffer, 0, len * 4);
    }
    /** Writes arbitrary number of 8-byte floats at once. */
    public void writeDoubles(double[] values, int off, int len) throws IOException {
        reserveConversionBufferSize(len * 8);
        ByteBuffer.wrap(conversionBuffer).asDoubleBuffer().put(values, off, len);
        writeBytes(conversionBuffer, 0, len * 8);
    }
    
    public static final Charset CHARSET = Charset.forName("UTF-8");
}
