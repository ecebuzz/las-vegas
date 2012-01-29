package edu.brown.lasvegas.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;

import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Encapsulates file reads from an LVFS-managed file in a remote data node.
 */
public class DataNodeFileInputStream extends VirtualFileInputStream {
    public DataNodeFileInputStream (Configuration conf, LVDataProtocol dataNode, String localPath) throws IOException {
        int[] statuses = dataNode.getCombinedFileStatus(localPath);
        boolean exists = statuses[1] != 0;
        if (!exists) {
            throw new FileNotFoundException("this file doesn't exist in the data node:" + localPath);
        }
        boolean directory = statuses[2] != 0;
        if (directory) {
            throw new IOException ("it's a folder! " + localPath);
        }
        RemoteInputStream inFile = dataNode.getFileInputStream(localPath);
        wrapped = RemoteInputStreamClient.wrap(inFile);
    }
    private final InputStream wrapped;
    
    @Override
    public int read() throws IOException {
        return wrapped.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return wrapped.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return wrapped.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return wrapped.skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapped.available();
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    @Override
    public void mark(int readlimit) {
        wrapped.mark(readlimit);
    }
    @Override
    public boolean markSupported() {
        return wrapped.markSupported();
    }

    @Override
    public void reset() throws IOException {
        wrapped.reset();
    }
}
