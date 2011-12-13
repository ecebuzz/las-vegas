package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.List;

/**
 * This dummy class proxies {@link #chooseTarget(String, int, DatanodeDescriptor, long)}
 * to a _protected_ abstract function. As the original function is package-private,
 * our own class can't implement BlockPlacementPolicy otherwise. Grrrrrr...
 */
public abstract class ProxyBlockPlacementPolicy extends BlockPlacementPolicy {
    @Override
    final DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes, long blocksize) {
        return chooseTargetP(srcPath, numOfReplicas, writer, chosenNodes, blocksize);
    }
    /**
     * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
     * to re-replicate a block with size <i>blocksize</i> 
     * If not, return as many as we can.
     * 
     * @param srcPath the file to which this chooseTargets is being invoked. 
     * @param numOfReplicas additional number of replicas wanted.
     * @param writer the writer's machine, null if not in the cluster.
     * @param chosenNodes datanodes that have been chosen as targets.
     * @param blocksize size of the data to be written.
     * @return array of DatanodeDescriptor instances chosen as target 
     * and sorted as a pipeline.
     */
    protected abstract DatanodeDescriptor[] chooseTargetP(String srcPath, int numOfReplicas, DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes, long blocksize);
}
