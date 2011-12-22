package edu.brown.lasvegas.protocol;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Defines a protocol to receive load/replication/recovery requests
 * for LVFS files.
 */
public interface LVDataProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
}
