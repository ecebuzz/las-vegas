package edu.brown.lasvegas.lvfs.placement;

import java.io.IOException;

import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVTable;

/**
 * This interface defines the events on which we re-consider
 * the file placement.
 * 
 * <p>At each re-consideration point, we want to optimize the
 * amount of data to transmit (considering where the current files are),
 * the expected query performance,
 * and future recovery time. This is obviously NP-hard, and also
 * requires read main metadata objects. Thus, we do NOT modify
 * placements in the most granular levels of events. The events defined
 * in this interface is rather coarse and infrequent.</p>
 * 
 * <p>Also, we do not pursue the really optimal decision. It's hard to compute.
 * Instead, we follow a few rules to simplify the problem, such as not moving
 * any existing files (which might not be a really good choice, but simplifies
 * it a lot!). </p>
 */
public interface PlacementEventHandler {
    /**
     * Called when a new rack is added to the network (or recovered to normal status).
     * 
     * <p>When this event happens, we do the following. For each fracture of all tables,
     * we re-balance the rack assignment (LVRackAssinment), meaning we assign this new rack to the
     * replica group with the least racks assigned. If the rack is the first
     * assigned rack for the replica group (in other words, the replica group wasn't materialized
     * until now), we do the same as {@link #onNewFracture(LVTable, LVFracture)} regarding
     * this rack.
     * </p>
     */
    void onNewRack(LVRack rack) throws IOException;
    
    /**
     * Called when a new fracture is added.
     * 
     * <p>When this event happens, we do the following. First, we assign the racks
     * to replica groups in the table. For each replica group that gets some rack assignment
     * for the new fracture, we list all the active nodes in the assigned rack(s) and the number
     * of replica partitions they store.</p>
     * 
     * <p>The node list is sorted by the number so that we assign
     * more replica partitions to empty nodes as far as we don't violate the following.
     * Rule 1: The same sub-partition of two replica schemes must not reside in the same node.
     * Rule 2: If there are 3 or more replica schemes, and 2 or more racks, balance the number
     * of same sub-partitions in each rack. 
     * </p>
     * TODO : Rule 2 is arguable. Placing all buddies in the same rack will boost recovery performance.
     * However, it will be much more risky regarding rack failure. To be discussed.
     * 
     * <p>With the above rules, we simply iterate for sub-partitions, check the current file location,
     * and then pick the new location in a round-robin fashion.</p>
     */
    void onNewFracture (LVFracture fracture) throws IOException;
    
    /**
     * Called when an entire rack is lost. Hopefully, this event is much much more rare than {@link #onLostRackNode(LVRackNode)}.
     * 
     * <p>Upon a lost rack, we recover files from other racks. This method determines
     * where to recover each file. The actual recovery will happen subsequently, guided
     * by the new locations stored in the metadata store. </p>
     */
    void onLostRack (LVRack rack) throws IOException;
    
    /**
     * Called when a node is lost. This will be the most frequent failure event.
     * 
     * <p>Upon a lost node, we recover files in the following preference order.
     * 1: from other node in the same rack. 2: from other node in other rack assigned to the same
     * replica group. 3: from a bunch of nodes in other rack assigned to other replica group. </p>
     */
    void onLostRackNode (LVRackNode node) throws IOException;
}
