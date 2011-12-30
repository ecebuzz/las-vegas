package edu.brown.lasvegas.lvfs.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * Implementation of VirtualFile that wraps HDFS.
 */
public final class HdfsVirtualFile implements VirtualFile {
    private final FileSystem hdfs;
    private Path path;
    public HdfsVirtualFile (FileSystem hdfs, Path path) {
        this.hdfs = hdfs;
        this.path = path;
    }
    public HdfsVirtualFile (FileSystem hdfs, String path) {
        this (hdfs, new Path(path));
    }
    public HdfsVirtualFile (String path) throws IOException {
        this (new Path(path));
    }
    public HdfsVirtualFile (Path path) throws IOException {
        this (path.getFileSystem(new Configuration()), path);
    }
    
    private final static int INPUT_BUFFER_SIZE = 1 << 20;
    private final static int OUTPUT_BUFFER_SIZE = 1 << 20;
    private final static short DEFAULT_REPLICATION_FACTOR = 1;
    private final static long DEFAULT_BLOCK_SIZE = 64L << 20;
    
    @Override
    public VirtualFileInputStream getInputStream() throws IOException {
        return new HdfsVirtualFileInputStream(hdfs.open(path, INPUT_BUFFER_SIZE));
    }

    @Override
    public VirtualFileOutputStream getOutputStream() throws IOException {
        return new HdfsVirtualFileOutputStream(
            hdfs.create(path, true, OUTPUT_BUFFER_SIZE, DEFAULT_REPLICATION_FACTOR, DEFAULT_BLOCK_SIZE));
    }

    @Override
    public long length() throws IOException {
        return hdfs.getFileStatus(path).getLen();
    }

    @Override
    public boolean exists() throws IOException {
        return hdfs.exists(path);
    }
    @Override
    public boolean isDirectory() throws IOException {
        return hdfs.isDirectory(path);
    }
    @Override
    public boolean delete() throws IOException {
        return delete (false);
    }
    @Override
    public boolean delete(boolean recursive) throws IOException {
        return hdfs.delete(path, recursive);
    }

    @Override
    public VirtualFile getParentFile() {
        Path parentPath = path.getParent();
        if (parentPath == null) {
            return null;
        }
        return new HdfsVirtualFile(hdfs, parentPath);
    }
    @Override
    public VirtualFile getChildFile(String filename) {
        String childPath = path.toString() + "/" + filename;
        return new HdfsVirtualFile(hdfs, childPath);
    }

    @Override
    public boolean mkdirs() throws IOException {
        return hdfs.mkdirs(path);
    }
    @Override
    public boolean renameTo(VirtualFile newPath) throws IOException {
        Path dest = ((HdfsVirtualFile) newPath).path;
        boolean success = hdfs.rename(path, dest);
        if (success) {
            path = dest;
        }
        return success;
    }

    @Override
    public String getAbsolutePath() {
        return path.toString();
    }
    @Override
    public String getName() {
        return path.getName();
    }

    
    @Override
    public String toString() {
        return "HDFSFile:" + getAbsolutePath();
    }
}
