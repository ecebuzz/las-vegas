package edu.brown.lasvegas.lvfs.local;

import java.io.FileOutputStream;
import java.io.IOException;

import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * An implementation of VirtualFileOutputStream that delegates everything except {@link #syncDurable()} to FileOutputStream.
 */
public final class LocalVirtualFileOutputStream extends VirtualFileOutputStream {
    private final FileOutputStream stream;
    public LocalVirtualFileOutputStream (FileOutputStream stream) {
        this.stream = stream;
    }
    @Override
    public void syncDurable() throws IOException {
        stream.getFD().sync();
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
