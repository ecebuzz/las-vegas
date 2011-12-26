package edu.brown.lasvegas.protocol;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Defines a protocol to receive load/replication/recovery requests
 * for LVFS files.
 */
public interface LVDataProtocol extends VersionedProtocol {

    /**
     * Returns the byte size of the specified LVFS file in the data node.
     * @param localPath file path in the data node.
     * @return size in byte. ok to be a 4 bytes integer because it's a partitioned column file
     * @throws IOException If the file doesn't exist or isn't readable.
     */
    int getFileLength (String localPath) throws IOException;
    
    /**
     * Returns the data of the specified LVFS file in the data node.
     * This is much simpler than what HDFS's DFSInputStream does.
     * There might be some limitation because of that, but so far a simple interface suffices.
     * @param localPath file path in the data node.
     * @param offset the byte offset to start reading from
     * @param len the byte size to read.
     * @return read bytes
     * @throws IOException If the file doesn't exist, isn't readable or the given offset/len is out of range.
     */
    byte[] getFileBody (String localPath, int offset, int len) throws IOException;
    
    /** Returns whether the specified file exists in the data node. */
    boolean existsFile (String localPath) throws IOException;
    /** Returns whether the specified file exists in the data node and also is a folder. */
    boolean isDirectory (String localPath) throws IOException;
    
    /**
     * getFileLength() + exists() + isDirectory().
     * This one method returns 3 properties of the specified file to reduce RPC calls.
     * @param localPath file path in the data node.
     * @return 3 integers. file length, exists(1=true/0=false), directory (1=true/0=false). 
     * @throws IOException
     */
    int[] getCombinedFileStatus (String localPath) throws IOException;

    /**
     * Deletes a file or folder in the data node.
     * @param localPath file path in the data node.
     * @param recursive whether to delete children if the file is a folder.
     * @return whether deleted the file (if the file didn't exist, false)
     * @throws IOException
     */
    boolean deleteFile (String localPath, boolean recursive) throws IOException;
    
    /**
     * Stops this data node.
     */
    void shutdown () throws IOException;
    
    public static final long versionID = 1L;
}
