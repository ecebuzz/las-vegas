package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.LVQueryProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Encapsulates connection to the query execution service.
 */
public final class LVQueryClient extends ClientBase<LVQueryProtocol> {
    /**
     * Connects to the query execution service.
     */
    public LVQueryClient(Configuration conf) throws IOException {
        super (LVQueryProtocol.class, LVQueryProtocol.versionID, conf, LVCentralNode.QE_ADDRESS_KEY, LVCentralNode.QE_ADDRESS_DEFAULT);
    }
}
