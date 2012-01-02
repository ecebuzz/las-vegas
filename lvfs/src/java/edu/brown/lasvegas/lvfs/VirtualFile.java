package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.lvfs.hdfs.HdfsVirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.util.URLVirtualFile;

/**
 * Represents a file in LVFS which might be a local file or an RPC
 * connection to a remote data node.
 * @see LocalVirtualFile
 * @see DataNodeFile
 * @see HdfsVirtualFile
 * @see URLVirtualFile
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
    long length() throws IOException;
    
    /**
     * Returns if the file exists.
     */
    boolean exists() throws IOException;
    
    /**
     * Returns if the file is a folder.
     */
    boolean isDirectory() throws IOException;
    
    /**
     * Deletes the file from the underlying filesystem and returns if the file is actually deleted.
     * @param recursive if the file is a folder, whether to recursively deletes the content.
     */
    boolean delete(boolean recursive) throws IOException;
    /**
     * Overload that assumes the file is not a folder (not recursive deletion).
     */
    boolean delete() throws IOException;
    
    /**
     * Returns the parent file object.
     * If the current file is root, returns null.
     */
    VirtualFile getParentFile ();
    
    /**
     * Returns a file with the given name in the folder.
     */
    VirtualFile getChildFile (String filename);
    
    /**
     * If this file is a folder, creates all folders up to this folder.
     * @return if the operation succeeded.
     */
    boolean mkdirs() throws IOException;
    
    /**
     * Moves this file to the new path.
     * @return if the operation succeeded.
     */
    boolean renameTo (VirtualFile newPath) throws IOException;
    
    /**
     * Returns the unique path to identify the file.
     * It might be a path in actual file system, or a path in HDFS.
     */
    String getAbsolutePath ();

    /**
     * Returns the file name of the file itself (not full path).
     */
    String getName();
}
