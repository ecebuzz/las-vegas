package edu.brown.lasvegas.lvfs.data;

import java.io.File;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * All tasks and modules in data node shares this context. 
 */
public class DataEngineContext {
    /** the directory in the local file system which contains all files managed by LVFS. */
    public File localLvfsRootDir;
    /** the directory in the local file system which contains all tentative files managed by LVFS. */
    public File localLvfsTmpDir;
    
    /** metadata repository. */
    public LVMetadataProtocol metaRepo;
    public Configuration conf;
    /** ID of LVRackNode this engine is running on. */
    public int nodeId;
}
