package edu.brown.lasvegas.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.data.DataEngine;
import edu.brown.lasvegas.protocol.LVDataProtocol;

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
<quote>
&lt;property&gt;
  &lt;name&gt;dfs.datanode.plugins&lt;/name&gt;
  &lt;value&gt;edu.brown.lasvegas.server.LVFSDataNode&lt;/value&gt;
&lt;/property&gt;
</quote>
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

    /** hadoop configuration */
    private Configuration conf;
    public Configuration getConfiguration() {
        return conf;
    }

    /** HDFS Data Node containing this object as a plugin. */
    private DataNode hdfsDataNode;

    public static final String DATA_ADDRESS_KEY = "lasvegas.server.data.address";
    public static final String DATA_ADDRESS_DEFAULT = "localhost:28712";
    
    /** server object to provide data accesses on LVFS files in this node. */
    private Server dataServer;
    /** instance of the LVFS data management in this node. */
    private DataEngine dataEngine;

    public static LVDataNode createInstance(Configuration conf) throws IOException {
        LVDataNode instance = new LVDataNode();
        try {
            instance.initialize(conf);
        } catch (Exception exception) {
            LOG.error("error while initializing data node. will terminate", exception);
            try {
                instance.stop();
            } catch (Exception ex2) {
                LOG.error("another error while stopping", ex2);
            }
            throw new IOException("error while initializing data node", exception);
        }
        return instance;
    }

    private void initialize(Configuration conf) throws IOException {
        String address = conf.get(DATA_ADDRESS_KEY, DATA_ADDRESS_DEFAULT);
        LOG.info("initializing LVFS Data Server. address=" + address);
        InetSocketAddress sockAddress = NetUtils.createSocketAddr(address);
        dataEngine = new DataEngine();
        dataServer = RPC.getServer(LVDataProtocol.class, dataEngine, sockAddress.getHostName(), sockAddress.getPort(), conf);
        LOG.info("initialized LVFS Data Server.");
        dataServer.start();
        LOG.info("started LVFS Data Server.");
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void start(Object service) {
        // TODO Auto-generated method stub
        hdfsDataNode = (DataNode) service;
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
        if (dataServer != null) {
            dataServer.stop();
            try {
                dataEngine.close();
            } catch (IOException ex) {
                LOG.error("error on closing data engine", ex);
            }
        }
        LOG.info("Stopped data node.");
    }
}
