package edu.brown.lasvegas.lvfs.hdfs;

import org.apache.hadoop.conf.Configuration;
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
        dfsCluster = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(2)
            .format(true).racks(null).build();
    }

    @AfterClass
    public static void tearDownOnce () {
        dfsCluster.shutdown();
        dfsCluster = null;
    }
}
