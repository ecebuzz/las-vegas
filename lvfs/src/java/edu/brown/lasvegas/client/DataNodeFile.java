package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;
import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Encapsulates an LVFS-managed file in a remote data node.
 * As LVDataProtocol provides only read accesses (doesn't need most of writes from remote nodes),
 * this VirtualFile doesn't support OutputStream.
 */
public class DataNodeFile implements VirtualFile {
    public DataNodeFile (LVDataProtocol dataNode, String localPath) {
        this (new Configuration(), dataNode, localPath);
    }
    public DataNodeFile (Configuration conf, LVDataProtocol dataNode, String localPath) {
        this.conf = conf;
        this.dataNode = dataNode;
        this.localPath = localPath;
    }
    
    private final Configuration conf;
    private final LVDataProtocol dataNode;
    private final String localPath;
    
    @Override
    public VirtualFileInputStream getInputStream() throws IOException {
        return new DataNodeFileInputStream(conf, dataNode, localPath);
    }

    @Override
    public VirtualFileOutputStream getOutputStream() throws IOException {
        throw new IOException ("output not supported");
    }

    @Override
    public long length() throws IOException {
        return dataNode.getFileLength(localPath);
    }

    @Override
    public boolean exists() throws IOException {
        return dataNode.existsFile(localPath);
    }

    @Override
    public boolean isDirectory() throws IOException {
        return dataNode.isDirectory(localPath);
    }

    @Override
    public boolean delete(boolean recursive) throws IOException {
        return dataNode.deleteFile(localPath, recursive);
    }

    @Override
    public boolean delete() throws IOException {
        return delete(false);
    }

    @Override
    public VirtualFile getParentFile() {
        int lastSl = localPath.lastIndexOf('/');
        if (lastSl < 0) {
            return null;
        }
        String parentPath = localPath.substring(0, lastSl);
        return new DataNodeFile(dataNode, parentPath);
    }
    @Override
    public VirtualFile getChildFile(String filename) {
        String childPath = localPath + "/" + filename;
        return new DataNodeFile(dataNode, childPath);
    }

    @Override
    public boolean mkdirs() throws IOException {
        throw new IOException ("mkdirs not supported");
    }
    @Override
    public boolean renameTo(VirtualFile newPath) throws IOException {
        throw new IOException ("rename not supported");
    }

    @Override
    public String getAbsolutePath() {
        return localPath;
    }

    @Override
    public String getName() {
        int lastSl = localPath.lastIndexOf('/');
        if (lastSl < 0) {
            return localPath;
        }
        return localPath.substring(lastSl + 1);
    }

    
    @Override
    public String toString() {
        return "DataNodeFile:" + getAbsolutePath();
    }
}
