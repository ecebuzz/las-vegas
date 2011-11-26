package edu.brown.lasvegas.lvfs.local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.RawValueReader;

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
    
    private final RawValueReader reader;
    /**
     * Returns the internal reader object to given read values.
     * Usually this is only used from derived classes, but testcases also use it.
     */
    public final RawValueReader getRawValueReader () {
        return reader;
    }

    private final int streamBufferSize;
    /** input stream of the raw file. */
    private InputStream rawStream;
    /** current byte position of the input stream. */
    private long curPosition;
    /** returns current byte position of the input stream. */
    public long getCurPosition() {
        return curPosition;
    }
    
    /**
     * Instantiates a new local raw file reader.
     *
     * @param rawFile the raw file
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public LocalRawFileReader (File rawFile, int streamBufferSize) throws IOException {
        this.rawFile = rawFile;
        rawFileSize = rawFile.length();
        this.streamBufferSize = streamBufferSize;
        if (streamBufferSize > 0) {
            rawStream = new BufferedInputStream(new FileInputStream(rawFile), streamBufferSize);
        } else {
            rawStream = new FileInputStream(rawFile);
        }
        curPosition = 0;
        reader = new RawValueReader() {
            @Override
            public int readBytes(byte[] buf, int off, int len) throws IOException {
                int read = rawStream.read(buf, off, len);
                if (read < 0) {
                    throw new IOException ("EOF " + this);
                }
                assert (read == len);
                curPosition += read;
                return read;
            }
            @Override
            public void skipBytes(long length) throws IOException {
                if (length < 0) {
                    throw new IOException ("negative skip length:" + length);
                }
                if (length == 0) {
                    return;
                }
                // InputStream#skip() might skip fewer bytes than requested for legitimate reasons
                // ex. buffered stream reached the end of the buffer.
                // So, we need to repeatedly call it. (negative return is definitely an error though)
                for (long totalSkipped = 0; totalSkipped < length;) {
                    long skippedByte = rawStream.skip(length - totalSkipped);
                    if (skippedByte < 0) {
                        throw new IOException ("failed to skip??");
                    }
                    totalSkipped += skippedByte;
                    assert (totalSkipped <= length);
                }
                curPosition += length;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("skipped " + length + " bytes");
                }
            }
            @Override
            public boolean hasMore() throws IOException {
                return curPosition < rawFileSize;
            }
        };
        if (LOG.isInfoEnabled()) {
            LOG.info("opened raw file:" + this);
        }
    }
    
    private void reopenStream () throws IOException {
        rawStream.close();
        if (streamBufferSize > 0) {
            rawStream = new BufferedInputStream(new FileInputStream(rawFile), streamBufferSize);
        } else {
            rawStream = new FileInputStream(rawFile);
        }
        curPosition = 0;
        LOG.info("reopened stream");
    }
    
    /**
     * Close the file handle and release all resources.
     */
    public final void close() throws IOException {
        if (rawStream != null) {
            rawStream.close();
            rawStream = null;
        }
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
        reader.skipBytes(bytePosition - curPosition);
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

    /** returns actual size of underlying file. */
    public long getRawFileSize() {
        return rawFileSize;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RawFileReader (" + rawFile.getAbsolutePath() + ", " + rawFileSize + ") curPos=" + curPosition;
    }
}
