package edu.brown.lasvegas.lvfs.placement;

/**
 * A list of nodes sorted by priority to place new files.
 * <p>The priority is based on the number of replica partitions stored in each node.
 * We place a new file to the most vacant (least replica partitions) node
 * that does not violate the following rule.</p>
 * <b>Rule: The same sub-partition of two replica schemes must not reside in the same node.</b>
 */
public final class RackNodePriorityQueue {
    /**
     * Determines the node to store the replica partition
     * @param partition the index of the partitioning key range of the new file. This parameter is used
     * to avoid placing replicas of same partition to the same node.
     * @return the node to place the file. 
     */
    public RackNodeUsage pickNode (int partition) {
        assert (nodes.length > 0);
        int picked = -1;
        // pick the most vacant node
        for (int i = 0; i < nodes.length; ++i) {
            // but avoids the node that already stores the partition (even if for other replica scheme)
            if (nodes[i].storedPartitions.contains(partition)) {
                continue;
            }
            picked = i;
            break;
        }
        // if there isn't any other option, use the most vacant one
        if (picked == -1) {
            picked = 0;
        }
        
        // increment the usage counter, and potentially push the element to the back
        RackNodeUsage node = nodes[picked];
        ++node.assignedCount;
        int moveBefore;
        for (moveBefore = picked + 1; moveBefore < nodes.length; ++moveBefore) {
            if (nodes[moveBefore].assignedCount >= node.assignedCount) {
                break;
            }
        }
        
        // shift the array to keep it sorted by assignedCount
        for (int i = picked; i < moveBefore - 1; ++i) {
            nodes[i] = nodes[i + 1];
        }
        nodes [moveBefore - 1] = node;
        return node;
    }

    /** the considered nodes sorted by the total number of replica partitions they store. */
    private RackNodeUsage[] nodes;
}
