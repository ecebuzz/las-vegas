package edu.brown.lasvegas.lvfs.protocol;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * API to access all metadata (e.g., file names,
 * partitioning and sorting) in LVFS.
 */
public interface LVFSNameProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
}
