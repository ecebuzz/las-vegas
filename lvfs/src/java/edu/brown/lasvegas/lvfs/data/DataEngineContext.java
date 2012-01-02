package edu.brown.lasvegas.lvfs.data;

import java.io.File;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * All tasks and modules in data node shares this context. 
 */
public class DataEngineContext {
    public DataEngineContext(int nodeId, Configuration conf, LVMetadataProtocol metaRepo, File localLvfsRootDir, File localLvfsTmpDir) {
        this.nodeId = nodeId;
        this.conf = conf;
        this.metaRepo = metaRepo;
        this.localLvfsRootDir = localLvfsRootDir;
        this.localLvfsTmpDir = localLvfsTmpDir;
    }
    
    /** ID of LVRackNode this engine is running on. */
    public final int nodeId;
    /** hadoop configuration. */
    public final Configuration conf;
    /** metadata repository. */
    public final LVMetadataProtocol metaRepo;
    /** the directory in the local file system which contains all files managed by LVFS. */
    public final File localLvfsRootDir;
    /** the directory in the local file system which contains all tentative files managed by LVFS. */
    public final File localLvfsTmpDir;
}
