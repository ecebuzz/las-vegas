package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.ClearAllTest;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Used from several single-node benchmarks/tests.
 * 
 * This class isn't a based class for the individual benchmark classes because
 * those classes might already have their base class.
 * Even not, has-a is more flexible than is-a.
 * So, this class is supposed to be owned by the benchmark classes.
 */
public class SingleNodeBenchmarkResources {
    public static final String TEST_BDB_HOME = "test/bdb_data";
    public static final String LVFS_ROOT = "test";

    public final MasterMetadataRepository metaRepo;

    public final int rackCount;
    public final int databaseCount;

    public final Configuration[][] dataNodeConfs;
    public final LVDataNode[][] dataNodes;

    public final LVRack[] racks;
    public final LVRackNode[][] nodes;
    public final LVDatabase[] databases;

    /** constructor to use an existing repository. */
    public SingleNodeBenchmarkResources() throws IOException {
        this.metaRepo = new MasterMetadataRepository(false, TEST_BDB_HOME); // keep existing
        this.racks = metaRepo.getAllRacks();
        assert (racks.length > 0);
        this.rackCount = racks.length;
        this.databases = metaRepo.getAllDatabases();
        this.databaseCount = databases.length;

        this.dataNodes = new LVDataNode[rackCount][];
        this.dataNodeConfs = new Configuration[rackCount][];
        this.nodes = new LVRackNode[rackCount][];
        for (int i = 0; i < rackCount; ++i) {
            nodes[i] = metaRepo.getAllRackNodes(racks[i].getRackId());
            final int nodesPerRack = nodes[i].length;
            dataNodes[i] = new LVDataNode[nodesPerRack];
            dataNodeConfs[i] = new Configuration[nodesPerRack];
            for (int j = 0; j < nodesPerRack; ++j) {
                LVRackNode node = nodes[i][j];
                assert (node.getNodeId() == 1 + i * nodesPerRack + j);
                assert (node.getAddress().equals("localhost:" + (12350 + i * nodesPerRack + j)));
                Configuration conf = new Configuration();
                dataNodeConfs[i][j] = conf;
                String rootDir = LVFS_ROOT + "/node_" + i + "_"+ j + "_lvfs";
                String tmpDir = rootDir + "/tmp";
                conf.set(DataEngine.LOCAL_LVFS_ROOTDIR_KEY, rootDir);
                conf.set(DataEngine.LOCAL_LVFS_TMPDIR_KEY, tmpDir);
                conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
                conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
                conf.set(LVDataNode.DATA_ADDRESS_KEY, node.getAddress());
                conf.set(LVDataNode.DATA_NODE_NAME_KEY, node.getName());
                conf.set(LVDataNode.DATA_RACK_NAME_KEY, racks[i].getName());
                dataNodes[i][j] = new LVDataNode(conf, metaRepo);
                dataNodes[i][j].start(null);
            }
        }
    }
    /** constructor to newly create the repository. */
    public SingleNodeBenchmarkResources(int rackCount, int nodesPerRack, int databaseCount) throws IOException {
        ClearAllTest.deleteFileRecursive(new File(LVFS_ROOT));
        this.metaRepo = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke it
        this.rackCount = rackCount;
        this.databaseCount = databaseCount;

        this.databases = new LVDatabase[databaseCount];
        for (int i = 0; i < databaseCount; ++i) {
            databases[i] = metaRepo.createNewDatabase("db" + i);
            assert (databases[i] != null);
        }

        this.dataNodes = new LVDataNode[rackCount][nodesPerRack];
        this.dataNodeConfs = new Configuration[rackCount][nodesPerRack];
        this.racks = new LVRack[rackCount];
        this.nodes = new LVRackNode[rackCount][nodesPerRack];
        for (int i = 0; i < rackCount; ++i) {
            racks[i] = metaRepo.createNewRack("rack" + i);
            for (int j = 0; j < nodesPerRack; ++j) {
                LVRackNode node = metaRepo.createNewRackNode(racks[i], "node_" + i + "_" + j, "localhost:" + (12350 + i * nodesPerRack + j));
                assert (node.getNodeId() == 1 + i * nodesPerRack + j);
                nodes[i][j] = node;
                Configuration conf = new Configuration();
                dataNodeConfs[i][j] = conf;
                String rootDir = LVFS_ROOT + "/node_" + i + "_"+ j + "_lvfs";
                String tmpDir = rootDir + "/tmp";
                conf.set(DataEngine.LOCAL_LVFS_ROOTDIR_KEY, rootDir);
                conf.set(DataEngine.LOCAL_LVFS_TMPDIR_KEY, tmpDir);
                conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
                conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
                conf.set(LVDataNode.DATA_ADDRESS_KEY, node.getAddress());
                conf.set(LVDataNode.DATA_NODE_NAME_KEY, node.getName());
                conf.set(LVDataNode.DATA_RACK_NAME_KEY, racks[i].getName());
                dataNodes[i][j] = new LVDataNode(conf, metaRepo);
                dataNodes[i][j].start(null);
            }
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        tearDown ();
    }
    
    private boolean released = false;
    public void tearDown () throws IOException {
        if (!released) {
            for (LVDataNode[] array : dataNodes) {
                for (LVDataNode node : array) {
                    node.close();
                }
            }
            metaRepo.shutdown();
            released = true;
        }
    }
}
