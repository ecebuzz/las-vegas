package edu.brown.lasvegas.lvfs.data;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Implementation of {@link LVDataProtocol}.
 * 
 */
public final class DataEngine implements LVDataProtocol, Closeable {
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        if (protocol.equals(LVDataProtocol.class.getName())) {
            return LVDataProtocol.versionID;
        } else {
            throw new IOException("This protocol is not supported: " + protocol);
        }
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public byte[] getFileBody(String localPath, int offset, int len) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public int getFileLength(String localPath) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }
}
