package edu.brown.lasvegas.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.protocol.MetadataProtocol;

/**
 * The central node that maintains metadata of files and initiates
 * replication and recovery of them.
 * <p>There must be only one central LVFS node in the system.</p>
 * 
 * <p>The central node provides multiple protocols for clients; metadata repository
 * (MetadataProtocol), TODO XXX, and YYY. This class itself doesn't implement
 * the protocols, but holds Server objects for each of the protocol it provides.</p>
 */
public final class CentralNode {
    private static Logger LOG = Logger.getLogger(CentralNode.class);

    static{
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
        Configuration.addDefaultResource("lasvegas-default.xml");
        Configuration.addDefaultResource("lasvegas-site.xml");
    }
    // private Daemon replicator;

    /** hadoop configuration */
    private Configuration conf;
    public Configuration getConfiguration() {
        return conf;
    }
    
    public static final String METAREPO_ADDRESS_KEY = "lasvegas.server.meta.address";
    public static final String METAREPO_ADDRESS_DEFAULT = "localhost:28710";
    public static final String METAREPO_BDBHOME_KEY = "lasvegas.server.meta.bdbhome";
    public static final String METAREPO_BDBHOME_DEFAULT = "metarepo/bdb_data";
    
    /** server object to provide metadata repository access to clients. */
    private Server metadataRepositoryServer;
    /** instance of the master metadata repository running in this central node. */
    private MasterMetadataRepository metadataRepository;
    
    /** disabled. */
    private CentralNode() {}

    /**
     * Creates, initializes and returns an LVFS node on this machine. 
     */
    public static CentralNode createInstance() throws IOException {
        return createInstance (new Configuration());
    }
    public static CentralNode createInstance(Configuration conf) throws IOException {
        CentralNode instance = new CentralNode();
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
        {
            // initialize metadata repository server
            String bdbHome = conf.get(METAREPO_BDBHOME_KEY, METAREPO_BDBHOME_DEFAULT);
            String address = conf.get(METAREPO_ADDRESS_KEY, METAREPO_ADDRESS_DEFAULT);
            LOG.info("initializing metadata repository server. address=" + address + ", bdbHome=" + bdbHome);
            InetSocketAddress sockAddress = NetUtils.createSocketAddr(address);
            metadataRepository = new MasterMetadataRepository(false, bdbHome);
            metadataRepositoryServer = RPC.getServer(MetadataProtocol.class, metadataRepository, sockAddress.getHostName(), sockAddress.getPort(), conf);
            LOG.info("initialized metadata repository server.");
            metadataRepositoryServer.start();
            LOG.info("started metadata repository server.");
        }
        /*
        replicator = new Daemon(new Replicator());
        replicator.start();
        */
    }

    private boolean stopRequested = false;
    /**
     * Requests all modules in this node to stop.
     */
    public void stop() {
        if (stopRequested) {
            return;
        }
        LOG.info("Stopping central node...");
        stopRequested = true;
        if (metadataRepositoryServer != null) {
            metadataRepositoryServer.stop();
            try {
                metadataRepository.close();
            } catch (IOException ex) {
                LOG.error("error on closing metadata repository", ex);
            }
        }
        LOG.info("Stopped central node.");
    }
    /**
     * Block until all modules in this node stop.
     */
    public void join() {
        LOG.info("Waiting until all modules in central node stop...");
        try {
            if (metadataRepositoryServer != null) {
                metadataRepositoryServer.join();
            }
            LOG.info("All modules in central node have been stopped.");
        } catch (InterruptedException ex) {
            LOG.warn("Interrupted while joining modules in central node.", ex);
        }
    }
/*
    class Replicator implements Runnable {
        @Override
        public void run() {
            // TODO Auto-generated method stub
        }
    }
    */
}
