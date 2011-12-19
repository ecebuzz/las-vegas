package edu.brown.lasvegas.lvfs.server;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.util.Daemon;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.protocol.LVFSDataProtocol;
import edu.brown.lasvegas.lvfs.protocol.LVFSNameProtocol;

/**
 * The central LVFS node that maintains metadata of LVFS files and initiates
 * replication and recovery of them.
 * There must be only one central LVFS node in the system.
 * 
 * LVFSNode accepts data reads/writes requests from clients
 * as an enhanced DFS server.
 */
public final class LVFSCentralNode implements LVFSDataProtocol, LVFSNameProtocol {
    private static Logger LOG = Logger.getLogger(LVFSCentralNode.class);

    static{
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }
    private Daemon replicator;

    /** hadoop configuration */
    private Configuration conf;
    
    /** disabled. */
    private LVFSCentralNode() {}

    /**
     * Creates, initializes and returns an LVFS node on this machine. 
     */
    public static LVFSCentralNode createInstance(Configuration conf) throws IOException {
        LVFSCentralNode instance = new LVFSCentralNode();
        try {
            instance.initialize(conf);
        } catch (Exception exception) {
            LOG.error("error while initializing central node. will terminate", exception);
            try {
                instance.stop();
            } catch (Exception ex2) {
                LOG.error("another error while stopping", ex2);
            }
            throw new IOException("error while initializing central node", exception);
        }
        return instance;
    }

        
    private void initialize(Configuration conf) throws IOException {
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

    /**
     * Requests all modules in this node to stop.
     */
    public void stop() {
        
    }
    /**
     * Block until all modules in this node stop.
     */
    public void join() {
        // TODO Auto-generated method stub
    }

    class Replicator implements Runnable {
        @Override
        public void run() {
            // TODO Auto-generated method stub
        }
    }
}
