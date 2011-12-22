package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * Encapsulates connection to the metadata repository service.
 */
public final class LVMetadataClient extends ClientBase<LVMetadataProtocol> {
    /**
     * Connects to the metadata repository service.
     */
    public LVMetadataClient(Configuration conf) throws IOException {
        super (LVMetadataProtocol.class, LVMetadataProtocol.versionID, conf, LVCentralNode.METAREPO_ADDRESS_KEY, LVCentralNode.METAREPO_ADDRESS_DEFAULT);
    }
}
