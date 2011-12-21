package edu.brown.lasvegas.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.MetadataProtocol;
import edu.brown.lasvegas.server.CentralNode;

/**
 * Encapsulates connection to the metadata repository service.
 */
public final class MetadataClient extends ClientBase<MetadataProtocol> {
    /**
     * Connects to the metadata repository service.
     */
    public MetadataClient(Configuration conf) throws IOException {
        super (MetadataProtocol.class, MetadataProtocol.versionID, conf, CentralNode.METAREPO_ADDRESS_KEY, CentralNode.METAREPO_ADDRESS_DEFAULT);
    }
}
