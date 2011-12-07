package edu.brown.lasvegas.lvfs;

import java.io.IOException;

/**
 * Represents a file in LVFS which might be a local file or a input/output
 * stream over HDFS.
 */
public interface VirtualFile {
    /**
     * Returns an input stream to read from the file.
     */
    VirtualFileInputStream getInputStream() throws IOException;
    
    /**
     * Returns an output stream to write to the file.
     */
    VirtualFileOutputStream getOutputStream() throws IOException;
    
    /**
     * Returns the length of the file.
     */
    long length();
    
    /**
     * Returns if the file exists.
     */
    boolean exists();
    
    /**
     * Deletes the file from the underlying filesystem and returns if the file is actually deleted.
     */
    boolean delete();
    
    /**
     * Returns the parent file object.
     */
    VirtualFile getParentFile ();
    
    /**
     * If this file is a folder, creates all folders up to this folder.
     */
    boolean mkdirs();
    
    /**
     * Returns the unique path to identify the file.
     * It might be a path in actual file system, or a path in HDFS.
     */
    String getAbsolutePath ();
}
