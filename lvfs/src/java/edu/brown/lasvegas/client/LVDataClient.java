package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Encapsulates connection to the data node.
 * Data node is not the central server, so the user has to provide
 * the address to connect.
 */
public final class LVDataClient extends ClientBase<LVDataProtocol> {
    /**
     * Connects to the query execution service.
     * @param dataNodeAddress address (host:port) of the data node to connect
     */
    public LVDataClient(Configuration conf, String dataNodeAddress) throws IOException {
        super (LVDataProtocol.class, LVDataProtocol.versionID, conf, dataNodeAddress);
    }
}
