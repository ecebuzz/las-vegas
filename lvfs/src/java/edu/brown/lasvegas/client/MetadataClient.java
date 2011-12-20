package edu.brown.lasvegas.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import edu.brown.lasvegas.protocol.MetadataProtocol;
import edu.brown.lasvegas.server.CentralNode;

/**
 * Encapsulates connection to the metadata repository service.
 */
public final class MetadataClient {
    /** The object to remotely call the metadata repository service. */
    private MetadataProtocol channel;
    
    /**
     * Connects to the metadata repository service.
     */
    public MetadataClient(Configuration conf) throws IOException {
        String address = conf.get(CentralNode.METAREPO_ADDRESS_KEY, CentralNode.METAREPO_ADDRESS_DEFAULT);
        MetadataProtocol proxy = RPC.getProxy(MetadataProtocol.class, MetadataProtocol.versionID, NetUtils.createSocketAddr(address), conf);
        RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetry(20, 200, TimeUnit.MILLISECONDS);
        channel = (MetadataProtocol) RetryProxy.create(MetadataProtocol.class, proxy, retryPolicy);
    }
    
    /**
     * Call this method to release all resources in this object.
     */
    public void release () {
        if (channel != null) {
            RPC.stopProxy(channel);
            channel = null;
        }
    }
    
    /**
     * Gets the interface to remotely call the metadata repository service.
     *
     * @return the interface to remotely call the metadata repository service
     */
    public MetadataProtocol getChannel() {
        return channel;
    }
}
