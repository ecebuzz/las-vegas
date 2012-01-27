package edu.brown.lasvegas.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.protocol.LVQueryProtocol;
import edu.brown.lasvegas.qe.QueryExecutionEngine;

/**
 * The central node that serves all centralized functionalities in Las-Vegas.
 * <p>There must be only one central LV node in the system.</p>
 * 
 * <p>The central node provides multiple protocols for clients; metadata repository
 * (MetadataProtocol), Query Parser/Optimizer, and Data Load/Recovery Engine.
 * This class itself doesn't implement
 * the protocols, but holds Server objects for each of the protocol it provides.</p>
 * 
 * <p>
 * Currently, the LV Central Node runs on HDFS name node.
 * It's deployed as a plugin ({@link ServicePlugin}) for HDFS Name Node.
 * </p>
 * 
 * <p>In order to activate it, the user needs to put the following parameter
 * in Hadoop's configuration file (e.g., hdfs-default.xml).</p>
<quote>
&lt;property&gt;
  &lt;name&gt;dfs.namenode.plugins&lt;/name&gt;
  &lt;value&gt;edu.brown.lasvegas.server.LVCentralNode&lt;/value&gt;
&lt;/property&gt;
</quote>
 */
public final class LVCentralNode implements ServicePlugin {
    private static Logger LOG = Logger.getLogger(LVCentralNode.class);

    static{
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    public LVCentralNode () {
        this(new Configuration());
    }
    public LVCentralNode (Configuration conf) {
        this (conf, false);
    }
    /**
     * @param formatMetarepo clears the existing meta repository on startup.
     */
    public LVCentralNode (Configuration conf, boolean formatMetarepo) {
        this.conf = conf;
        this.formatMetarepo = formatMetarepo;
    }
    private final boolean formatMetarepo;

    /** hadoop configuration */
    private Configuration conf;
    public Configuration getConfiguration() {
        return conf;
    }
    
    public static final String METAREPO_ADDRESS_KEY = "lasvegas.server.meta.address";
    public static final String METAREPO_ADDRESS_DEFAULT = "localhost:28710";
    public static final String METAREPO_BDBHOME_KEY = "lasvegas.server.meta.bdbhome";
    public static final String METAREPO_BDBHOME_DEFAULT = "metarepo/bdb_data";
    public static final String QE_ADDRESS_KEY = "lasvegas.server.qe.address";
    public static final String QE_ADDRESS_DEFAULT = "localhost:28711";
    
    /** server object to provide metadata repository access to clients. */
    private Server metadataRepositoryServer;
    /** instance of the master metadata repository running in this central node. */
    private MasterMetadataRepository metadataRepository;
    
    /** server object to receive query execution requests from clients. */
    private Server queryExecutionServer;
    /** instance of the query execution engine running in this central node.*/
    private QueryExecutionEngine queryExecutionEngine;

    /** the thread to periodically check if metadataRepository has shutdown. */
    private ShutdownPollingThread shutdownPollingThread;

    /** HDFS Name Node containing this object as a plugin. */
    private NameNode hdfsNameNode;
    public NameNode getHdfsNameNode () {
        return hdfsNameNode;
    }

    /**
     * Creates, initializes and activates the central node on this machine. 
     */
    @Override
    public void start(Object service) {
        hdfsNameNode = (NameNode) service;
        try {
            initialize();
        } catch (Exception exception) {
            LOG.error("error while initializing central node. will terminate", exception);
            try {
                stop();
            } catch (Exception ex2) {
                LOG.error("another error while stopping", ex2);
            }
            throw new RuntimeException("error while initializing central node", exception);
        }
    }

    private void initialize() throws IOException {
        {
            // initialize metadata repository server
            String bdbHome = conf.get(METAREPO_BDBHOME_KEY, METAREPO_BDBHOME_DEFAULT);
            String address = conf.get(METAREPO_ADDRESS_KEY, METAREPO_ADDRESS_DEFAULT);
            LOG.info("initializing metadata repository server. address=" + address + ", bdbHome=" + bdbHome);
            InetSocketAddress sockAddress = NetUtils.createSocketAddr(address);
            metadataRepository = new MasterMetadataRepository(formatMetarepo, bdbHome);
            metadataRepositoryServer = RPC.getServer(LVMetadataProtocol.class, metadataRepository, sockAddress.getHostName(), sockAddress.getPort(), conf);
            LOG.info("initialized metadata repository server.");
            metadataRepositoryServer.start();
            LOG.info("started metadata repository server.");
        }
        {
            // initialize query execution engine
            String address = conf.get(QE_ADDRESS_KEY, QE_ADDRESS_DEFAULT);
            LOG.info("initializing query execution engine server. address=" + address);
            InetSocketAddress sockAddress = NetUtils.createSocketAddr(address);
            queryExecutionEngine = new QueryExecutionEngine(metadataRepository);
            queryExecutionServer = RPC.getServer(LVQueryProtocol.class, queryExecutionEngine, sockAddress.getHostName(), sockAddress.getPort(), conf);
            LOG.info("initialized query execution engine  server.");
            queryExecutionServer.start();
            LOG.info("started query execution engine  server.");
        }
        shutdownPollingThread = new ShutdownPollingThread();
        shutdownPollingThread.start();
    }
    
    @Override
    public void close() throws IOException {
        stop ();
        hdfsNameNode = null;
        shutdownPollingThread = null;
    }

    private boolean stopRequested = false;
    /**
     * Requests all modules in this node to stop.
     */
    @Override
    public void stop() {
        if (stopRequested) {
            return;
        }
        LOG.info("Stopping central node...");
        stopRequested = true;
        // stop proceeds in the _opposite_ order to initialization
        if (queryExecutionServer != null) {
            queryExecutionServer.stop();
            try {
                queryExecutionEngine.shutdown();
            } catch (IOException ex) {
                LOG.error("error on closing query execution engine", ex);
            }
        }
        if (metadataRepositoryServer != null) {
            metadataRepositoryServer.stop();
            try {
                if (!metadataRepository.isShutdown()) {
                    metadataRepository.shutdown();
                }
                assert (metadataRepository.isShutdown());
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
            // stop proceeds in the _opposite_ order to initialization
            if (queryExecutionServer != null) {
                queryExecutionServer.join();
            }
            if (metadataRepositoryServer != null) {
                metadataRepositoryServer.join();
            }
            LOG.info("All modules in central node have been stopped.");
        } catch (InterruptedException ex) {
            LOG.warn("Interrupted while joining modules in central node.", ex);
        }
    }

    private class ShutdownPollingThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                }
                if (metadataRepository.isShutdown()) {
                    LOG.info("Metadata repository has been already shutdown. closing the central node...");
                    try {
                        close ();
                    } catch (Exception ex) {
                        LOG.error("error on closing central node", ex);
                    }
                    break;
                }
            }
        }
    }
}
