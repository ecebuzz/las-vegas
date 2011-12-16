package edu.brown.lasvegas.lvfs.placement;

import java.util.Comparator;

import edu.brown.lasvegas.LVRackNode;

/**
 * Represents how vacant a node is.
 */
public class RackNodeUsage {
    public RackNodeUsage (LVRackNode node, int assignedCount) {
        this.node = node;
        this.assignedCount = assignedCount;
    }
    
    public final LVRackNode node;
    /** total number of replica partitions this node stores.*/
    public int assignedCount;

    /** comparator to sort by assignedCount. uses nodeId if same count. */
    public final static class UsageComparator implements Comparator<RackNodeUsage> {
        @Override
        public int compare(RackNodeUsage o1, RackNodeUsage o2) {
            if (o1.assignedCount != o2.assignedCount) {
                return o1.assignedCount - o2.assignedCount;
            }
            return o1.node.getNodeId() - o2.node.getNodeId();
        }
    }
}