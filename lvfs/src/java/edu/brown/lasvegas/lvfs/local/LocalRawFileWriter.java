package edu.brown.lasvegas.lvfs.local;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

/**
 * Writes a write-once local data file.
 * All data files are in a plain format where
 * each entry is stored contiguously. No dictionary,
 * no index whatever. Those additional things are
 * stored in separate files.
 */
public class LocalRawFileWriter {
    protected static Logger LOG = Logger.getLogger(LocalRawFileWriter.class);

    /** underlying file handle. */
    protected final File file;
    
    /** output stream of the raw file. */
    protected final OutputStream stream;
    protected long curPosition = 0L;

    public LocalRawFileWriter (File file, int bufferSize) throws IOException {
        this.file = file;
        if (bufferSize <= 0) {
            stream = new FileOutputStream (file);
        } else {
            stream = new BufferedOutputStream(new FileOutputStream(file), bufferSize);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("created file:" + file.getAbsolutePath());
        }
    }

    public void flush () throws IOException {
        stream.flush();
        if (LOG.isDebugEnabled()) {
            LOG.debug("flushed file:" + file.getAbsolutePath());
        }
    }
    public void close () throws IOException {
        stream.flush();
        stream.close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("closed file:" + file.getAbsolutePath());
        }
    }

    /** Writes arbitrary byte array. */
    public final void writeBytes (byte[] buf, int off, int len) throws IOException {
        stream.write(buf, off, len);
        curPosition += len; // stream.write is always followed by curPosition increment
    }

    /** Writes 1 byte. */
    public final void writeByte (byte v) throws IOException {
        stream.write(v);
        ++curPosition;// stream.write is always followed by curPosition increment
    }

    /** Writes 1 byte  (so far we don't compress 8 booleans into 1 byte) as boolean. */
    public final void writeBoolean (boolean v) throws IOException {
        writeByte(v ? (byte) 1 : (byte) 0);
    }
    private final byte[] smallBuf = new byte[8];

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
    public final void readDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }
}
