package edu.brown.lasvegas.lvfs.server;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.util.Daemon;

import edu.brown.lasvegas.lvfs.protocol.LVFSDataProtocol;
import edu.brown.lasvegas.lvfs.protocol.LVFSNameProtocol;

/**
 * An LVFS node maintains LVFS files and deals with
 * replication and recovery of them.
 * 
 * LVFSNode accepts data reads/writes requests from clients
 * as an enhanced DFS server.
 */
public class LVFSNode implements LVFSDataProtocol, LVFSNameProtocol {
    private Daemon replicator;
    
    /** disabled. */
    private LVFSNode() {}

    /**
     * Creates, initializes and returns an LVFS node on this machine. 
     */
    public static LVFSNode createInstance(Configuration conf) {
        LVFSNode instance = new LVFSNode();
        instance.initialize(conf);
        return instance;
    }

    private void initialize(Configuration conf) {
        replicator = new Daemon(new Replicator());
        replicator.start();
    }
    
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    class Replicator implements Runnable {
        @Override
        public void run() {
            // TODO Auto-generated method stub
        }
    }
}
