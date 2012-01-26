package edu.brown.lasvegas.client;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Encapsulates file reads from an LVFS-managed file in a remote data node.
 */
public class DataNodeFileInputStream extends VirtualFileInputStream {
    public static final String BUFFER_SIZE_KEY = "lasvegas.client.data.read_buffer_size";
    public static final int BUFFER_SIZE_DEFAULT = 1 << 20;
    
    public DataNodeFileInputStream (Configuration conf, LVDataProtocol dataNode, String localPath) throws IOException {
        this.conf = conf;
        this.dataNode = dataNode;
        this.localPath = localPath;
        int[] statuses = dataNode.getCombinedFileStatus(localPath);
        fileLength = statuses[0];
        boolean exists = statuses[1] != 0;
        if (!exists) {
            throw new FileNotFoundException("this file doesn't exist in the data node:" + localPath);
        }
        boolean directory = statuses[2] != 0;
        if (directory) {
            throw new IOException ("it's a folder! " + localPath);
        }
        maxBufferSize = this.conf.getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
    }
    private final Configuration conf;
    private final LVDataProtocol dataNode;
    private final String localPath;
    private final int maxBufferSize;
    private byte[] buffer;
    
    /** the current byte position in the entire file. */
    private int cursorInFile = 0;
    /** the byte position corresponding to the beginning of the current buffer. */
    private int bufferStartPosition = 0;
    /** byte size of the entire file. */
    private final int fileLength;

    /** previously marked byte position in the entire file. */
    private int markedPosition = 0;
    
    @Override
    public int read() throws IOException {
        fetchIfNeeded();
        if (cursorInFile >= fileLength) {
            return -1;
        }
        int ret = (int) buffer[cursorInFile - bufferStartPosition];
        assert (ret >= 0);
        ++cursorInFile;
        return ret;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        assert (b.length >= off + len);
        if (cursorInFile + len > fileLength) {
            len = fileLength - cursorInFile;
        }
        int totalRead = 0; 
        while (totalRead < len) {
            fetchIfNeeded();
            int bufferPos = cursorInFile - bufferStartPosition;
            int bytesToRead = len - totalRead;
            if (bufferPos + bytesToRead > buffer.length) {
                bytesToRead = buffer.length - bufferPos;
            }
            System.arraycopy(buffer, bufferPos, b, off + totalRead, bytesToRead);
            totalRead += bytesToRead;
            cursorInFile += bytesToRead;
        }
        return totalRead;
    }

    @Override
    public long skip(long n) throws IOException {
        assert (n <= 0x7FFFFFFF);
        int skippedLen = (int) n;
        if (skippedLen > fileLength - cursorInFile) {
            skippedLen = fileLength - cursorInFile;
        }
        cursorInFile += skippedLen;
        return skippedLen;
    }

    @Override
    public int available() throws IOException {
        return fileLength - cursorInFile;
    }

    @Override
    public void close() throws IOException {
        // do nothing. the LVDataProtocol is completely state-less for file reads (so far).
    }

    @Override
    public synchronized void mark(int readlimit) {
        markedPosition = cursorInFile;
    }
    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void reset() throws IOException {
        cursorInFile = markedPosition;
    }
    
    private void fetchIfNeeded () throws IOException {
        if (needsFetch()) {
            fetch (cursorInFile);
        }
    }
    private boolean needsFetch () {
        if (buffer == null) {
            return true;
        }
        if (cursorInFile >= fileLength) {
            return false;
        }
        return (cursorInFile < bufferStartPosition || cursorInFile >= bufferStartPosition + buffer.length);
    }
    
    private void fetch (int newBufferStartPosition) throws IOException {
        int len = maxBufferSize;
        if (len > fileLength - newBufferStartPosition) {
            len = fileLength - newBufferStartPosition;
        }
        buffer = dataNode.getFileBody(localPath, newBufferStartPosition, len);
        assert (buffer.length == len);
        bufferStartPosition = newBufferStartPosition;
    }
}
