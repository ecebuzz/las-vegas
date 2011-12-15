package edu.brown.lasvegas.lvfs.placement;

import java.util.HashSet;

import edu.brown.lasvegas.LVSubPartitionScheme;

/**
 * Represents how vacant a node is.
 */
public class RackNodeUsage {
    public int nodeId;
    /** total number of replica partitions this node stores.*/
    public int assignedCount;

    /** sub-partitioning scheme the following attributes are about. */
    public int subPartitionSchemeId;
    /**
     * Sub-partitions (indexes of ranges in {@link LVSubPartitionScheme}) stored in this node.
     * This is about a particular sub-partitioning scheme specified in subPartitionSchemeId.
     * So, don't use an instance of this object over multiple replica groups!
     */
    public HashSet<Integer> storedPartitions = new HashSet<Integer>();
}