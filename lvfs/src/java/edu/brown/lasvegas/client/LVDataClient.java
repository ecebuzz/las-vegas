package edu.brown.lasvegas.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import edu.brown.lasvegas.protocol.LVDataProtocol;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Encapsulates connection to the data node.
 * Data node is not the central server, so the user has to provide
 * the address to connect.
 */
public final class LVDataClient /*extends ClientBase<LVDataProtocol>*/ {
    /**
     * Connects to the query execution service.
     * @param dataNodeAddress address (host:port) of the data node to connect
     */
    public LVDataClient(Configuration conf, String dataNodeAddress) throws IOException {
        InetSocketAddress sockAddress = NetUtils.createSocketAddr(dataNodeAddress);
        Registry registry = LocateRegistry.getRegistry(sockAddress.getHostName(), sockAddress.getPort());
        String serviceName = LVDataNode.DATA_ENGINE_SERVICE_NAME;
        try {
            channel = (LVDataProtocol) registry.lookup(serviceName);
        } catch (NotBoundException ex) {
            throw new IOException ("Couldn't connect to the data node: " + dataNodeAddress, ex);
        }
        // super (LVDataProtocol.class, LVDataProtocol.versionID, conf, dataNodeAddress);
        /*
        proxy = RPC.getProxy(protocolClass, protocolVersionID, NetUtils.createSocketAddr(address), conf);
        // RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetry(20, 200, TimeUnit.MILLISECONDS);
        RetryPolicy retryPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
        channel = (T) RetryProxy.create(protocolClass, proxy, retryPolicy);
         */
    }
    private LVDataProtocol channel;
    
    /**
     * Call this method to release all resources in this object.
     */
    public void release () {
        channel = null;
    }

    /**
     * Gets the RPC channel.
     * @return the RPC channel
     */
    public LVDataProtocol getChannel() {
        return channel;
    }
}
