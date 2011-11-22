package edu.brown.lasvegas.lvfs;

import java.io.IOException;
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

    /** Reads 1 byte  (so far we don't compress 8 booleans into 1 byte) and returns it as boolean. */
    public final boolean readBoolean () throws IOException {
        byte b = readByte();
        return b != (byte) 0;
    }
    /** Reads 1 byte and returns it as byte. */
    public final byte readByte () throws IOException {
        readBytes (smallBuf, 0, 1);
        return smallBuf[0];
    }

    /** Reads 2 bytes and returns it as short. */
    public final short readShort () throws IOException {
        readBytes (smallBuf, 0, 2);
        return (short)((((int) smallBuf[0]) << 8) + ((int) smallBuf[1] & 255));
    }
    
    /** Reads 4 bytes and returns it as int. */
    public final int readInt () throws IOException {
        readBytes (smallBuf, 0, 4);
        return ((((int) smallBuf[0]) << 24) + (((int) smallBuf[1] & 255) << 16) + (((int) smallBuf[2] & 255) << 8) + ((int) smallBuf[3] & 255));
    }
    
    /** Reads 8 bytes and returns it as long. */
    public final long readLong () throws IOException {
        readBytes (smallBuf, 0, 8);
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

    public final String readStringWithLengthHeader () throws IOException {
        return new String (readBytesWithLengthHeader(), CHARSET);
    }
    public final byte[] readBytesWithLengthHeader () throws IOException {
        int length = readLengthHeader();
        byte[] buf = new byte[length];
        int read = readBytes(buf, 0, length);
        if (read < length) {
            throw new IOException ("coudln't read part of " + length + " bytes entry??");
        }
        return buf;
    }
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

    public static final Charset CHARSET = Charset.forName("UTF-8");
}
