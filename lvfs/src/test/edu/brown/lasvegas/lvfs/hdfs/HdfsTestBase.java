package edu.brown.lasvegas.lvfs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Testcases that initialize and destroy MiniDfsCluster only once.
 * It's cleaner to initialize/destroy at each setup/teardown, but it increases
 * test runtime and log messages.
 */
public class HdfsTestBase {
    protected static MiniDFSCluster dfsCluster = null;
    
    @BeforeClass
    public static void setUpOnce () throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY, 10000); // to speed-up testing
        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
            .format(true).racks(null).build();
    }

    @AfterClass
    public static void tearDownOnce () {
        dfsCluster.shutdown();
        dfsCluster = null;
    }
}
