package edu.brown.lasvegas.lvfs.local;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.RawValueWriter;

/**
 * Writes a write-once local data file.
 * All data files are in a plain format where
 * each entry is stored contiguously. No dictionary,
 * no index whatever. Those additional things are
 * stored in separate files.
 */
public class LocalRawFileWriter {
    private static Logger LOG = Logger.getLogger(LocalRawFileWriter.class);

    /** underlying file handle. */
    protected final File file;
    
    /** output stream of the raw file. */
    private final OutputStream stream;
    private final FileOutputStream fo;

    private final RawValueWriter writer;
    /**
     * Returns the internal writer object to receive written values.
     * Usually this is only used from derived classes, but testcases also use it.
     */
    public final RawValueWriter getRawValueWriter () {
        return writer;
    }

    private long curPosition = 0L;
    public final long getCurPosition () {
        return curPosition;
    }
    // no setter because only this class should maintain it

    public LocalRawFileWriter (File file, int bufferSize) throws IOException {
        this.file = file;
        if (file.exists()) {
            file.delete();
        }
        fo = new FileOutputStream (file, false);
        if (bufferSize <= 0) {
            stream = fo;
        } else {
            stream = new BufferedOutputStream(fo, bufferSize);
        }
        writer = new RawValueWriter() {
            @Override
            public void writeBytes(byte[] buf, int off, int len) throws IOException {
                stream.write(buf, off, len);
                curPosition += len;
            }
            @Override
            public void writeByte(byte v) throws IOException {
                stream.write(v);
                ++curPosition;
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("created file:" + file.getAbsolutePath());
        }
    }

    /**
     * this version only flushes the underlying stream, does not call sync.
     */
    public final void flush () throws IOException {
        flush (false);
    }
    /**
     * @param sync whether to call getFD().sync(). This makes sure the written
     * data is durable, but this might be costly. As Hadoop application does not 
     * need 100% ACID, asynchronous write by OS might be enough. 
     */
    public void flush (boolean sync) throws IOException {
        stream.flush(); // this flushes out the buffer, but still the stream might not be written out
        if (sync) {
            fo.getFD().sync(); // this really ensures the written data is durable.
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("flushed file:" + file.getAbsolutePath());
        }
    }
    /**
     * Close the file.
     */
    public void close () throws IOException {
        stream.close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("closed file:" + file.getAbsolutePath());
        }
    }

    /** Override this method if the file format needs some per-file footer. */
    public void writeFileFooter() throws IOException {}

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RawFileWriter (" + file.getAbsolutePath() + ") curPos=" + curPosition;
    }
}
