package edu.brown.lasvegas.lvfs.local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Reads a read-only local data file.
 * All data files are in a plain format where
 * each entry is stored contiguously. No dictionary,
 * no index whatever. Those additional things are
 * stored in separate files.
 */
public class LocalRawFileReader {
    private static Logger LOG = Logger.getLogger(LocalRawFileReader.class);

    /** underlying file handle. */
    private final File rawFile;
    /** actual size of underlying file. */
    private final long rawFileSize;
    
    /** input stream of the raw file. */
    private BufferedInputStream rawStream;
    /** current byte position of the input stream. */
    private long curPosition;
    
    private final static int STREAM_BUFFER_SIZE = 1 << 16;
    
    /**
     * Instantiates a new local raw file reader.
     *
     * @param rawFile the raw file
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public LocalRawFileReader (File rawFile) throws IOException {
        this.rawFile = rawFile;
        rawFileSize = rawFile.length();
        rawStream = new BufferedInputStream(new FileInputStream(rawFile), STREAM_BUFFER_SIZE);
        curPosition = 0;
        if (LOG.isDebugEnabled()) {
            LOG.debug("opened raw file:" + this);
        }
    }
    
    private void reopenStream () throws IOException {
        rawStream.close();
        rawStream = new BufferedInputStream(new FileInputStream(rawFile), STREAM_BUFFER_SIZE);
        curPosition = 0;
    }
    
    /**
     * Close the file handle and release all resources.
     */
    public void close() throws IOException {
        rawStream.close();
        rawStream = null;
    }
    
    /**
     * Jump to the desired absolute byte position. This method can jump to
     * previous position. In that case, this method re-opens the
     * input stream.
     * @param bytePosition moves the input stream cursor to this position
     * @throws IOException
     */
    public final void seekToByteAbsolute (long bytePosition) throws IOException {
        assert (bytePosition >= 0);
        if (bytePosition == curPosition) {
            return;
        }
        if (bytePosition < curPosition) {
            reopenStream ();
            assert (curPosition == 0L);
        }
        if (bytePosition >= rawFileSize) {
            LOG.warn("too large byte position. adjusted to file size " + bytePosition + "/" + rawFileSize + " at " + this);
            bytePosition = rawFileSize;
        }
        if (bytePosition == curPosition) {
            return;
        }
        
        long bytesToSkip = bytePosition - curPosition;
        assert (bytesToSkip > 0L);
        long skippedByte = rawStream.skip(bytesToSkip);
        if (bytesToSkip != skippedByte) {
            LOG.warn("unexpected skip behavior?? " + skippedByte + "/" + bytesToSkip + " at " + this);
        }
        curPosition += skippedByte;
        if (LOG.isDebugEnabled()) {
            LOG.debug("skipped " + skippedByte + " bytes: " + this);
        }
    }
    /**
     * Jump to the desired byte position relative to current position. This method can jump to
     * previous position. In that case, this method re-opens the
     * input stream.
     * @param bytesToSkip number of bytes to skip. negative values will reopen the file.
     * @throws IOException
     */
    public final void seekToByteRelative (long bytesToSkip) throws IOException {
        seekToByteAbsolute (curPosition + bytesToSkip);
    }
    /**
     * Reads len bytes to the given buffer.
     * @param buf buffer to receive the result
     * @param off offset of buf
     * @param len number of bytes to read
     * @return number of bytes actually read
     * @throws IOException
     */
    public final int readBytes (byte[] buf, int off, int len) throws IOException {
        int read = rawStream.read(buf, off, len);
        if (read < 0) {
            throw new IOException ("EOF " + this);
        }
        curPosition += read;
        return read;
    }

    /** Reads 1 byte  (so far we don't compress 8 booleans into 1 byte) and returns it as boolean. */
    public final boolean readBoolean () throws IOException {
        byte b = readByte();
        return b != (byte) 0;
    }
    /** Reads 1 byte and returns it as byte. */
    public final byte readByte () throws IOException {
        int read = rawStream.read();
        if (read < 0) {
            throw new IOException ("EOF " + this);
        }
        curPosition += 1;
        return (byte) read; // it's signed byte!
    }
    private final byte[] smallBuf = new byte[8];

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

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RawFileReader (" + rawFile.getAbsolutePath() + ", " + rawFileSize + ") curPos=" + curPosition;
    }

    /**
     * Gets the underlying file handle.
     *
     * @return the underlying file handle
     */
    public File getRawFile() {
        return rawFile;
    }

    /**
     * Gets the actual size of underlying file.
     *
     * @return the actual size of underlying file
     */
    public long getRawFileSize() {
        return rawFileSize;
    }

    /**
     * Gets the input stream of the raw file.
     *
     * @return the input stream of the raw file
     */
    /* encapsulate everything about raw stream in this class..
    public FileInputStream getRawStream() {
        return rawStream;
    }
    */

    /**
     * Gets the current byte position of the input stream.
     *
     * @return the current byte position of the input stream
     */
    public long getCurPosition() {
        return curPosition;
    }
}
