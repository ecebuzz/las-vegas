package edu.brown.lasvegas.util;

import java.io.IOException;
import java.io.InputStream;

import edu.brown.lasvegas.lvfs.VirtualFileInputStream;

/**
 * An implementation of VirtualFileInputStream that uses InputStream.
 */
public final class URLVirtualFileInputStream extends VirtualFileInputStream {
    private final InputStream stream;
    public URLVirtualFileInputStream(InputStream stream) {
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
