package edu.brown.lasvegas.lvfs.local;

import java.io.FileInputStream;
import java.io.IOException;

import edu.brown.lasvegas.lvfs.VirtualFileInputStream;

/**
 * An implementation of VirtualFileInputStream that simply delegates everything to FileInputStream.
 */
public final class LocalVirtualFileInputStream extends VirtualFileInputStream {
    private final FileInputStream stream;
    public LocalVirtualFileInputStream(FileInputStream stream) {
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
