package edu.brown.lasvegas.lvfs.placement;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import edu.brown.lasvegas.LVRackNode;

/**
 * A list of nodes sorted by priority to place new files.
 * <p>The priority is based on the number of replica partitions stored in each node.
 * We place a new file to the most vacant (least replica partitions) node
 * that does not violate the following rule.</p>
 * <b>Rule: The same sub-partition of two replica schemes must not reside in the same node.</b>
 * 
 * <p>As an optional rule, this queue returns the node with smaller ID if all the other conditions
 * are the same. This is not quite a required property, but handy for testcases.</p>
 */
public final class RackNodePriorityQueue {
    public RackNodePriorityQueue(Collection<RackNodeUsage> nodeCollection) {
        this.array = nodeCollection.toArray(new RackNodeUsage[0]);
        comparator = new RackNodeUsage.UsageComparator();
        Arrays.sort(array, comparator);
    }

    /**
     * Call this to clear usedNodeIdsForCurrentPartition.
     * @param usedNodeIds IDs of Nodes that already store some replica partition of the partition.
     * Such nodes are to not be assigned more replica partitions as much as possible. 
     */
    public void moveToNextPartition (Collection<Integer> usedNodeIds) {
        this.usedNodeIdsForCurrentPartition.clear();
        this.usedNodeIdsForCurrentPartition.addAll(usedNodeIds);
    }
    
    /**
     * Determines the node to store the replica partition
     * @return the node to place the file. 
     */
    public LVRackNode pickNode () {
        assert (array.length > 0);
        int picked = -1;
        // pick the most vacant node
        for (int i = 0; i < array.length; ++i) {
            // but avoids the node that already stores the partition (even if for other replica scheme)
            if (usedNodeIdsForCurrentPartition.contains(array[i].node.getNodeId())) {
                continue;
            }
            picked = i;
            break;
        }
        // if there isn't any other option, use the most vacant one (this violates the rule, but no other way)
        if (picked == -1) {
            picked = 0;
        }
        
        // increment the usage counter, and potentially push the element to the back
        RackNodeUsage node = array[picked];
        ++node.assignedCount;
        usedNodeIdsForCurrentPartition.add(node.node.getNodeId());
        int moveBefore;
        for (moveBefore = picked + 1; moveBefore < array.length; ++moveBefore) {
            int compared = comparator.compare(array[moveBefore], node);
            assert (compared != 0); // because comparator also checks ID
            if (compared > 0) {
                break;
            }
        }
        
        // shift the array to keep it sorted by assignedCount
        for (int i = picked; i < moveBefore - 1; ++i) {
            array[i] = array[i + 1];
        }
        array [moveBefore - 1] = node;
        return node.node;
    }

    /** the considered nodes sorted by the total number of replica partitions they store. */
    private final RackNodeUsage[] array;

    /**
     * Represents which node already stores some replica partition.
     * This is about a particular sub-partitioning scheme.
     * So, don't use an instance of this object over multiple replica groups!
     */
    private final HashSet<Integer> usedNodeIdsForCurrentPartition = new HashSet<Integer>();
    
    private final RackNodeUsage.UsageComparator comparator;
}
