package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

/**
 * Base class to wrap RPC connection.
 */
public class ClientBase<T> {
    /** The RPC channel. */
    private T channel;
    
    /** raw proxy object. */
    private T proxy;
    
    /**
     * Connects to the RPC service with address in configuration file.
     */
    protected ClientBase(Class<T> protocolClass, long protocolVersionID, Configuration conf, String addressConfKey, String addressConfDefault) throws IOException {
        this (protocolClass, protocolVersionID, conf, conf.get(addressConfKey, addressConfDefault));
    }
    /**
     * Connects to the RPC service with the given address.
     */
    @SuppressWarnings("unchecked")
    protected ClientBase(Class<T> protocolClass, long protocolVersionID, Configuration conf, String address) throws IOException {
        proxy = RPC.getProxy(protocolClass, protocolVersionID, NetUtils.createSocketAddr(address), conf);
        // RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetry(20, 200, TimeUnit.MILLISECONDS);
        RetryPolicy retryPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
        channel = (T) RetryProxy.create(protocolClass, proxy, retryPolicy);
    }
    
    /**
     * Call this method to release all resources in this object.
     */
    public void release () {
        if (channel != null) {
            RPC.stopProxy(proxy);
            channel = null;
            proxy = null;
        }
    }
    
    /**
     * Gets the RPC channel.
     * @return the RPC channel
     */
    public T getChannel() {
        return channel;
    }
}
