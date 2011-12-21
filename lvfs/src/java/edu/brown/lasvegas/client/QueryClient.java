package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.QueryProtocol;
import edu.brown.lasvegas.server.CentralNode;

/**
 * Encapsulates connection to the query execution service.
 */
public final class QueryClient extends ClientBase<QueryProtocol> {
    /**
     * Connects to the query execution service.
     */
    public QueryClient(Configuration conf) throws IOException {
        super (QueryProtocol.class, QueryProtocol.versionID, conf, CentralNode.QE_ADDRESS_KEY, CentralNode.QE_ADDRESS_DEFAULT);
    }
}
