package edu.brown.lasvegas.lvfs.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * An implementation of VirtualFileOutputStream that delegates everything to HDFS's FSDataOutputStream.
 */
public final class HdfsVirtualFileOutputStream extends VirtualFileOutputStream {
    private final FSDataOutputStream stream;
    public HdfsVirtualFileOutputStream (FSDataOutputStream stream) {
        this.stream = stream;
    }
    @Override
    public void syncDurable() throws IOException {
        stream.hsync();
    }
    @Override
    public void write(int b) throws IOException {
        stream.write(b);
    }
    @Override
    public void close() throws IOException {
        stream.close();
    }
    @Override
    public void flush() throws IOException {
        stream.flush();
    }
    @Override
    public void write(byte[] b) throws IOException {
        stream.write(b);
    }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        stream.write(b, off, len);
    }
}
