package edu.brown.lasvegas.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.DataEngine;
import edu.brown.lasvegas.protocol.LVDataProtocol;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Las-Vegas Data Node to receive load/replication/recovery requests
 * for LVFS files.
 * 
 * <p>Like {@link LVCentralNode}, this class merely forwards method
 * calls to the real implementation class, {@link DataEngine}.</p>

 * <p>
 * LVDataNode runs on each HDFS node to extend its functionality.
 * It's deployed as a plugin ({@link ServicePlugin}) for HDFS Data Node.
 * </p>
 * 
 * <p>In order to activate it, the user needs to put the following parameter
 * in Hadoop's configuration file (e.g., hdfs-default.xml).</p>
<pre>
&lt;property&gt;
  &lt;name&gt;dfs.datanode.plugins&lt;/name&gt;
  &lt;value&gt;edu.brown.lasvegas.server.LVFSDataNode&lt;/value&gt;
&lt;/property&gt;

&lt;property&gt;
  &lt;name&gt;lasvegas.server.data.node_name&lt;/name&gt;
  &lt;value&gt;node1&lt;/value&gt;
&lt;/property&gt;

&lt;property&gt;
  &lt;name&gt;lasvegas.server.data.rack_name&lt;/name&gt;
  &lt;value&gt;rack1&lt;/value&gt;
&lt;/property&gt;
</pre>
 * 
 */
public final class LVDataNode implements ServicePlugin {
    private static Logger LOG = Logger.getLogger(LVDataNode.class);

    static{
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    public LVDataNode () {
        this(new Configuration());
    }
    public LVDataNode (Configuration conf) {
        this (conf, null);
    }
    /** Constructor to directly specify the metadata repository instance. usually for testing. */
    public LVDataNode (Configuration conf, LVMetadataProtocol metaRepo) {
        this (conf, metaRepo, false, false);
    }
    /**
     * Constructor to specify whether to newly register this node/rack and to format the data root directory on startup.
     */
    public LVDataNode (Configuration conf, LVMetadataProtocol metaRepo, boolean registerRackNode, boolean format) {
        this.conf = conf;
        this.metaRepo = metaRepo;
        this.registerRackNode = registerRackNode;
        this.format = format;
    }
    private final boolean registerRackNode;
    private final boolean format;
    
    /** hadoop configuration */
    private Configuration conf;
    public Configuration getConfiguration() {
        return conf;
    }

    /** HDFS Data Node containing this object as a plugin. */
    private DataNode hdfsDataNode;
    public DataNode getHdfsDataNode () {
        return hdfsDataNode;
    }
    
    /** only if connecting to remote metadata repository. */
    private LVMetadataClient metaClient;
    /** metadata repository. */
    private LVMetadataProtocol metaRepo;

    public static final String DATA_ADDRESS_KEY = "lasvegas.server.data.address";
    public static final String DATA_ADDRESS_DEFAULT = "localhost:28712";

    public static final String DATA_NODE_NAME_KEY = "lasvegas.server.data.node_name";
    public static final String DATA_NODE_NAME_DEFAULT = "default_node";

    public static final String DATA_RACK_NAME_KEY = "lasvegas.server.data.rack_name";
    public static final String DATA_RACK_NAME_DEFAULT = "default_rack";
    
    /** server object to provide data accesses on LVFS files in this node. */
    //private Server dataServer;
    private Registry rmiRegistry;
    /** instance of the LVFS data management in this node. */
    private DataEngine dataEngine;
    /** the thread to periodically check if dataEngine has shutdown. */
    private ShutdownPollingThread shutdownPollingThread;

    @Override
    public void start(Object service) {
        hdfsDataNode = (DataNode) service;
        try {
            initialize();
        } catch (Exception exception) {
            LOG.error("error while initializing data node. will terminate", exception);
            try {
                stop();
            } catch (Exception ex2) {
                LOG.error("another error while stopping", ex2);
            }
            throw new RuntimeException("error while initializing data node", exception);
        }
    }

    private void initialize() throws IOException {
        String address = conf.get(DATA_ADDRESS_KEY, DATA_ADDRESS_DEFAULT);
        LOG.info("initializing LVFS Data Server. address=" + address);
        if (metaRepo == null) {
            metaClient = new LVMetadataClient(conf);
            metaRepo = metaClient.getChannel();
        } else {
            LOG.debug ("using metadata repository instance given to the constructor");
        }
        String rackName = conf.get(DATA_RACK_NAME_KEY, DATA_RACK_NAME_DEFAULT);
        LVRack rack = metaRepo.getRack(rackName);
        if (rack == null) {
            if (registerRackNode) {
                LOG.info("automatically creating LVRack:" + rackName);
                rack = metaRepo.createNewRack(rackName);
                if (rack == null) {
                    throw new IOException ("Failed to create a rack: " + rackName);
                }
            } else {
                throw new IOException ("This rack name doesn't exist: " + rackName);
            }
        }

        String nodeName = conf.get(DATA_NODE_NAME_KEY, DATA_NODE_NAME_DEFAULT);
        LVRackNode node = metaRepo.getRackNode(nodeName);
        if (node == null) {
            if (registerRackNode) {
                LOG.info("automatically creating LVRackNode:" + nodeName);
                node = metaRepo.createNewRackNode(rack, nodeName, address);
            } else {
                throw new IOException ("This node name doesn't exist: " + nodeName);
            }
        }
        if (node.getRackId() != rack.getRackId()) {
            throw new IOException ("This node doesn't belong to the specified rack:" + node + " : " + rackName);
        }
        if (node.getStatus() != RackNodeStatus.OK) {
            LOG.info("updating the status of the node");
            metaRepo.updateRackNodeStatusNoReturn(node.getNodeId(), RackNodeStatus.OK);
        }
        if (node.getAddress().equals(address)) {
            LOG.info("updating the address of the node");
            metaRepo.updateRackNodeAddressNoReturn(node.getNodeId(), address);
        }

        InetSocketAddress sockAddress = NetUtils.createSocketAddr(address);
        dataEngine = new DataEngine(metaRepo, node.getNodeId(), conf, format);
        dataEngine.start();
        
        LVDataProtocol dataProtocol = (LVDataProtocol) UnicastRemoteObject.exportObject(dataEngine, sockAddress.getPort());
        rmiRegistry = LocateRegistry.createRegistry(sockAddress.getPort());
        try {
            rmiRegistry.bind(DATA_ENGINE_SERVICE_NAME, dataProtocol);
        } catch (AlreadyBoundException ex) {
            LOG.warn("There seems a stale DataEngine. replacing with a new instance.", ex);
            rmiRegistry.rebind(DATA_ENGINE_SERVICE_NAME, dataProtocol);
        }
        // dataServer = RPC.getServer(LVDataProtocol.class, dataEngine, sockAddress.getHostName(), sockAddress.getPort(), conf);
        LOG.info("initialized LVFS Data Server.");
        //dataServer.start();
        //LOG.info("started LVFS Data Server.");
        
        shutdownPollingThread = new ShutdownPollingThread();
        shutdownPollingThread.start();
    }
    public static final String DATA_ENGINE_SERVICE_NAME = "LVDataEngineService";
    
    @Override
    public void close() throws IOException {
        stop ();
        hdfsDataNode = null;
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
        LOG.info("Stopping data node...");
        stopRequested = true;
        // stop proceeds in the _opposite_ order to initialization
        if (rmiRegistry != null) {
            try {
                rmiRegistry.unbind(DATA_ENGINE_SERVICE_NAME);
            } catch (Exception ex) {
                LOG.warn("error on unbinding RMI service for data engine. ignored", ex);
            }
            try {
                UnicastRemoteObject.unexportObject(rmiRegistry, true);
            } catch (NoSuchObjectException ex) {
                LOG.warn("error on closing RMI registry for data engine. ignored", ex);
            }
            rmiRegistry = null;
            try {
                if (!dataEngine.isShutdown()) {
                    dataEngine.close();
                }
                assert (dataEngine.isShutdown());
            } catch (IOException ex) {
                LOG.error("error on closing data engine", ex);
            }
        }
        if (metaClient != null) {
            metaClient.release();
            metaClient = null;
        }
        LOG.info("Stopped data node.");
    }

    /**
     * Block until all modules in this node stop.
     */
    public void join() {
        LOG.info("Waiting until data node stops...");
        try {
            if (rmiRegistry != null && shutdownPollingThread != null) {
                shutdownPollingThread.join();
            }
            LOG.info("data node has been stopped.");
        } catch (InterruptedException ex) {
            LOG.warn("Interrupted while joining data node.", ex);
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
                if (dataEngine.isShutdown()) {
                    LOG.info("Data engine has been already shutdown. closing the data node...");
                    try {
                        close ();
                    } catch (Exception ex) {
                        LOG.error("error on closing data engine", ex);
                    }
                    break;
                }
            }
        }
    }
}
