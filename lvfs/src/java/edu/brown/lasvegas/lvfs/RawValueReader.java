package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Interface to give raw values (no notion of tuples here) to read.
 * The implementation class defines where to get the data.
 */
public abstract class RawValueReader {
    private final byte[] smallBuf = new byte[8];
    /**
     * Reads len bytes to the given buffer.
     * @param buf buffer to receive the result
     * @param off offset of buf
     * @param len number of bytes to read
     * @return number of bytes actually read
     * @throws IOException
     */
    public abstract int readBytes (byte[] buf, int off, int len) throws IOException;

    /**
     * Jumps over the specified amount of bytes.
     * @param length byte size to skip
     * @throws IOException
     */
    public abstract void skipBytes(long length) throws IOException;
    
    /**
     * Returns whether there is still something to read.
     */
    public abstract boolean hasMore () throws IOException;

    /** Reads 1 byte  (so far we don't compress 8 booleans into 1 byte) and returns it as boolean. */
    public final boolean readBoolean () throws IOException {
        byte b = readByte();
        return b != (byte) 0;
    }
    /** Reads 1 byte and returns it as byte. */
    public abstract byte readByte () throws IOException;

    /** Reads 2 bytes and returns it as short. */
    public final short readShort () throws IOException {
        int read = readBytes (smallBuf, 0, 2);
        assert (read == 2);
        return (short)((((int) smallBuf[0]) << 8) + ((int) smallBuf[1] & 255));
    }
    
    /** Reads 4 bytes and returns it as int. */
    public final int readInt () throws IOException {
        int read = readBytes (smallBuf, 0, 4);
        assert (read == 4);
        return ((((int) smallBuf[0]) << 24) + (((int) smallBuf[1] & 255) << 16) + (((int) smallBuf[2] & 255) << 8) + ((int) smallBuf[3] & 255));
    }
    
    /** Reads 8 bytes and returns it as long. */
    public final long readLong () throws IOException {
        int read = readBytes (smallBuf, 0, 8);
        assert (read == 8);
        return (((long) smallBuf[0] << 56) +
            ((long)(smallBuf[1] & 255) << 48) +
            ((long)(smallBuf[2] & 255) << 40) +
            ((long)(smallBuf[3] & 255) << 32) +
            ((long)(smallBuf[4] & 255) << 24) +
            ((smallBuf[5] & 255) << 16) +
            ((smallBuf[6] & 255) <<  8) +
            ((smallBuf[7] & 255) <<  0));
    }

    /** Reads 4 bytes and returns it as float. */
    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    /** Reads 8 bytes and returns it as double. */
    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /** Read a variable-length String with length header. */
    public final String readStringWithLengthHeader () throws IOException {
        return new String (readBytesWithLengthHeader(), CHARSET);
    }
    /** Read a variable-length byte[] with length header. */
    public final byte[] readBytesWithLengthHeader () throws IOException {
        int length = readLengthHeader();
        byte[] buf = new byte[length];
        int read = readBytes(buf, 0, length);
        if (read < length) {
            throw new IOException ("coudln't read part of " + length + " bytes entry??");
        }
        return buf;
    }
    /** Read a length header. */
    public final int readLengthHeader () throws IOException {
        byte lengthSize = readByte();
        int length;
        switch (lengthSize) {
        case 1: length = readByte(); break;
        case 2: length = readShort(); break;
        case 4: length = readInt(); break;
        default:
            throw new IOException ("unexpected length size=" + lengthSize + ". corrupted file?");
        }
        if (length < 0) {
            throw new IOException ("negative value length=" + length + ". corrupted file?");
        }
        return length;
    }

    /** temporary buffer to do batch conversion. */
    private byte[] conversionBuffer = new byte[1024];
    private int readIntoConversionBuffer(int bytesToRead) throws IOException {
        if (bytesToRead > conversionBuffer.length) {
            conversionBuffer = new byte[bytesToRead];
        }
        return readBytes(conversionBuffer, 0, bytesToRead);
    }

    /** Reads arbitrary number of 2-byte integers at once. */
    public final int readShorts(short[] buffer, int off, int len) throws IOException {
        len = readIntoConversionBuffer(len * 2) / 2;
        ByteBuffer.wrap(conversionBuffer).asShortBuffer().get(buffer, off, len);
        return len;
    }

    /** Reads arbitrary number of 4-byte integers at once. */
    public final int readInts(int[] buffer, int off, int len) throws IOException {
        len = readIntoConversionBuffer(len * 4) / 4;
        ByteBuffer.wrap(conversionBuffer).asIntBuffer().get(buffer, off, len);
        return len;
    }

    /** Reads arbitrary number of 8-byte integers at once. */
    public final int readLongs(long[] buffer, int off, int len) throws IOException {
        len = readIntoConversionBuffer(len * 8) / 8;
        ByteBuffer.wrap(conversionBuffer).asLongBuffer().get(buffer, off, len);
        return len;
    }
    
    /** Reads arbitrary number of 4-byte floats at once. */
    public final int readFloats(float[] buffer, int off, int len) throws IOException {
        len = readIntoConversionBuffer(len * 4) / 4;
        ByteBuffer.wrap(conversionBuffer).asFloatBuffer().get(buffer, off, len);
        return len;
    }

    /** Reads arbitrary number of 8-byte floats at once. */
    public final int readDoubles(double[] buffer, int off, int len) throws IOException {
        len = readIntoConversionBuffer(len * 8) / 8;
        ByteBuffer.wrap(conversionBuffer).asDoubleBuffer().get(buffer, off, len);
        return len;
    }
    
    public static final Charset CHARSET = Charset.forName("UTF-8");
}
