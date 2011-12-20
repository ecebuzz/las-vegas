package edu.brown.lasvegas.protocol;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * API to access data in LVFS.
 * The implementation might be a remote LVFS daemon or a local
 * instance.
 */
public interface LVFSDataProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
}
