package edu.brown.lasvegas.lvfs.placement;

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

/**
 * Overrides HDFS's BlockPlacementPolicy.
 */
public final class LVFSBlockPlacementPolicy extends ProxyBlockPlacementPolicy {
    /** default constructor is used by BlockPlacementPolicy.getInstance(). */
    public LVFSBlockPlacementPolicy() {
    }

    /** the real initialization happens here. */
    @Override
    protected void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap) {
        // TODO Auto-generated method stub
        
    }
    

    @Override
    protected DatanodeDescriptor[] chooseTargetP(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
                    long blocksize) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
                    boolean returnChosenNodes, HashMap<Node, Node> excludedNodes, long blocksize) {
        // TODO Auto-generated method stub
        return null;
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
