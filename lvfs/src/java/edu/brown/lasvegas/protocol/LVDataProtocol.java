package edu.brown.lasvegas.protocol;

import java.io.IOException;
import java.rmi.Remote;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Defines a protocol to receive load/replication/recovery requests
 * for LVFS files.
 */
public interface LVDataProtocol extends Remote {

    /**
     * Returns the byte size of the specified LVFS file in the data node.
     * @param localPath file path in the data node.
     * @return size in byte (0 if not exists). ok to be a 4 bytes integer because it's a partitioned column file
     * @throws IOException
     */
    int getFileLength (String localPath) throws IOException;
    
    /**
     * Returns an input stream over RMI of the specified LVFS file in the data node.
     * @param localPath file path in the data node.
     * @return input stream to read the file
     * @throws IOException
     */
    RemoteInputStream getFileInputStream (String localPath) throws IOException;
    
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
