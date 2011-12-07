package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream for {@link VirtualFile}.
 */
public abstract class VirtualFileOutputStream extends OutputStream {
    /**
     * Makes sure the written data is durable all the way down to disk.
     * Usually, this means a call of getFD().sync() on the underlying file output
     * stream, which is VERY expensive.
     */
    public abstract void syncDurable () throws IOException;
}
