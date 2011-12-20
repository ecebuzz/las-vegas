package edu.brown.lasvegas.protocol;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Protocol to query and modify information of file placement.
 * The server of this protocol always resides in the same node
 * as the metadata server because each method call in this protocol
 * will internally check and modify several metadata objects.
 * Doing RPC for each of the metadata access would be quite slow.
 */
public interface PlacementProtocol extends VersionedProtocol {

}
