package edu.brown.lasvegas.lvfs.placement;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.ProxyBlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.namenode.FSInodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.client.LVMetadataClient;

/**
 * Overrides HDFS's BlockPlacementPolicy.
 */
public final class LVFSBlockPlacementPolicy extends ProxyBlockPlacementPolicy {
    private static Logger LOG = Logger.getLogger(LVFSBlockPlacementPolicy.class);

    /** default constructor is used by BlockPlacementPolicy.getInstance(). */
    public LVFSBlockPlacementPolicy() {
    }
    
    private LVMetadataClient metadataClient;
    
    public void release () {
        if (metadataClient != null) {
            LOG.info("releasing metadata client in block placement policy..");
            metadataClient.release();
            metadataClient = null;
            LOG.info("released.");
        }
    }

    /** the real initialization happens here. */
    @Override
    protected void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap) {
        LOG.info("initializing metadata client in block placement policy..");
        try {
            metadataClient = new LVMetadataClient(conf);
        } catch (IOException ex) {
            LOG.error("failed to initialize metadata client in block placement policy.", ex);
            throw new RuntimeException ("failed to initialize metadata client in block placement policy.", ex);
        }
        LOG.info("initialized.");
    }
    

    @Override
    protected DatanodeDescriptor[] chooseTargetP(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
                    long blocksize) {
        return chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, false, null, blocksize);
    }

    @Override
    public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
                    boolean returnChosenNodes, HashMap<Node, Node> excludedNodes, long blocksize) {
        String nodeName;
        try {
            nodeName = metadataClient.getChannel().queryColumnFilePlacement(srcPath);
        } catch (Exception ex) {
            LOG.error("failed to call queryColumnFilePlacement() in block placement policy.", ex);
            throw new RuntimeException ("failed to call queryColumnFilePlacement() in block placement policy.", ex);
        }
        if (nodeName == null) {
            // this means the file should be replicated to all nodes
            return chosenNodes.toArray(new DatanodeDescriptor[0]);
        } else {
            // place to the specified node
            for (DatanodeDescriptor node : chosenNodes) {
                if (node.name.equals(nodeName)) {
                    return new DatanodeDescriptor[]{node};
                }
            }
            LOG.error("The node '" + nodeName + "' doesn't exist in chosenNodes.");
            return new DatanodeDescriptor[0];
        }
    }

    @Override
    public int verifyBlockPlacement(String srcPath, LocatedBlock lBlk, int minRacks) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo srcInode, Block block, short replicationFactor,
                    Collection<DatanodeDescriptor> existingReplicas, Collection<DatanodeDescriptor> moreExistingReplicas) {
        // TODO Auto-generated method stub
        return null;
    }
}
