package edu.brown.lasvegas.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Daemon;
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
    private Daemon replicator;

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

    /** disabled. */
    private LVCentralNode() {}

    /**
     * Creates, initializes and returns an LVFS node on this machine. 
     */
    public static LVCentralNode createInstance() throws IOException {
        return createInstance (new Configuration());
    }
    public static LVCentralNode createInstance(Configuration conf) throws IOException {
        LVCentralNode instance = new LVCentralNode();
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
        /*
        replicator = new Daemon(new Replicator());
        replicator.start();
        */
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
                queryExecutionEngine.close();
            } catch (IOException ex) {
                LOG.error("error on closing query execution engine", ex);
            }
        }
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

    class Replicator implements Runnable {
        @Override
        public void run() {
            // TODO Auto-generated method stub
        }
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void start(Object service) {
        // TODO Auto-generated method stub
        
    }
    
}
