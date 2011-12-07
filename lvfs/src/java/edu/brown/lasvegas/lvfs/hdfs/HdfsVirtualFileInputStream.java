package edu.brown.lasvegas.lvfs.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import edu.brown.lasvegas.lvfs.VirtualFileInputStream;

/**
 * An implementation of VirtualFileInputStream that delegates everything to HDFS's FSDataInputStream.
 */
public final class HdfsVirtualFileInputStream extends VirtualFileInputStream {
    private final FSDataInputStream stream;
    public HdfsVirtualFileInputStream(FSDataInputStream stream) {
        this.stream = stream;
    }
    
    @Override
    public int read() throws IOException {
        return stream.read();
    }
    @Override
    public int available() throws IOException {
        return stream.available();
    }
    @Override
    public void close() throws IOException {
        stream.close();
    }
    @Override
    public int read(byte[] b) throws IOException {
        return stream.read(b);
    }
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }
    @Override
    public synchronized void reset() throws IOException {
        stream.reset();
    }
    @Override
    public long skip(long n) throws IOException {
        return stream.skip(n);
    }
}
