package edu.brown.lasvegas.lvfs.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * Implementation of VirtualFile that wraps HDFS.
 */
public final class HdfsVirtualFile implements VirtualFile {
    private final FileSystem hdfs;
    private final Path path;
    public HdfsVirtualFile (FileSystem hdfs, Path path) {
        this.hdfs = hdfs;
        this.path = path;
    }
    public HdfsVirtualFile (Path path) throws IOException {
        this.hdfs = path.getFileSystem(new Configuration());
        this.path = path;
    }

    /** @see DistributedFileSystem#close() */
    public void close () throws IOException {
        hdfs.close();
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
    public boolean delete() throws IOException {
        return delete (false);
    }
    @Override
    public boolean delete(boolean recursive) throws IOException {
        return hdfs.delete(path, recursive);
    }

    @Override
    public VirtualFile getParentFile() {
        return new HdfsVirtualFile(hdfs, path.getParent());
    }

    @Override
    public boolean mkdirs() throws IOException {
        return hdfs.mkdirs(path);
    }

    @Override
    public String getAbsolutePath() {
        return path.toString();
    }
}
