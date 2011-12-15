package edu.brown.lasvegas.lvfs.placement;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackAssignment;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVSubPartitionScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.RackStatus;
import edu.brown.lasvegas.lvfs.meta.MetadataRepository;

/**
 * The default implementation of {@link PlacementEventHandler}.
 * This object runs on the centralized server to control all events.
 * We may want to develop a distributed version later.
 */
public final class PlacementEventHandlerImpl implements PlacementEventHandler {
    private static Logger LOG = Logger.getLogger(PlacementEventHandlerImpl.class);
    /** the metadata store. */
    private final MetadataRepository repository;
    public PlacementEventHandlerImpl(MetadataRepository repository) {
        this.repository = repository;
    }
    
    
    @Override
    public void onNewRack(LVRack rack) throws IOException {
        LOG.info("rebalancing for new rack:" + rack);
        for (LVTable table : repository.getAllTables()) {
            LVReplicaGroup[] groups = repository.getAllReplicaGroups(table.getTableId());
            if (groups.length == 0) {
                LOG.warn("this table doesn't have replica groups defined yet. skipped: " + table);
                continue;
            }
            for (LVFracture fracture : repository.getAllFractures(table.getTableId())) {
                ReplicaGroupToAssignRack result = pickReplicaGroupToAssignRack (rack, table, fracture, groups);
                LVReplicaGroup group = result.group;
                // store the assignment to the metadata store
                repository.createNewRackAssignment(rack, fracture, group);
                if (result.rackCount == 0) {
                     // If the rack is the first assigned rack for the replica group (in other words, the replica group wasn't materialized
                     // until now), we do the same as {@link #onNewFracture(LVTable, LVFracture)} regarding
                     // this rack.
                    rebalanceReplicas (fracture, group); // this will utilize the newly assigned (thus vacant) rack
                } else {
                    // otherwise, do nothing. the newly assigned rack will be utilized on the next onNewFracture.
                    // we don't do anything here to keep it simple, and minimize the data transmission (in other words, not just an excuse!).
                }
            }
        }
    }
    private static class ReplicaGroupToAssignRack {
        LVReplicaGroup group;
        int rackCount;
    }
    /**
     * Pick the replica group that has currently the least racks assigned.
     */
    private ReplicaGroupToAssignRack pickReplicaGroupToAssignRack (LVRack rack, LVTable table, LVFracture fracture, LVReplicaGroup[] groups) throws IOException {
        HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>(); // map<groupId, count of rack assignments>
        for (LVReplicaGroup group : groups) {
            counts.put(group.getGroupId(), 0);
        }
        
        for (LVRackAssignment assignment : repository.getAllRackAssignmentsByFractureId(fracture.getFractureId())) {
            Integer currentCount = counts.get(assignment.getOwnerReplicaGroupId());
            if (currentCount == null) {
                LOG.warn("this rack assignment seems on a non-existing replica group.. wtf? : " + assignment);
                continue;
            }
            counts.put(assignment.getOwnerReplicaGroupId(), currentCount + 1);
        }
        
        int minCount = Integer.MAX_VALUE;
        LVReplicaGroup minGroup = null;
        for (LVReplicaGroup group : groups) {
            Integer count = counts.get(group.getGroupId());
            if (count < minCount) {
                minGroup = group;
                minCount = count;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("picked replica group: " + minGroup + " to assign " + rack);
        }
        assert (minGroup != null);
        ReplicaGroupToAssignRack result = new ReplicaGroupToAssignRack();
        result.group = minGroup;
        result.rackCount = minCount;
        return result;
    }
    
    @Override
    public void onNewFracture(LVFracture fracture) throws IOException {
        LOG.info("rebalancing for new fracture:" + fracture);
        
        // First, we assign the racks to replica groups in the table.
        LVTable table = repository.getTable(fracture.getTableId());
        LVReplicaGroup[] groups = repository.getAllReplicaGroups(fracture.getTableId());
        HashSet<LVReplicaGroup> assignedGroups = new HashSet<LVReplicaGroup>(); // groups that got some rack assignment.
        for (LVRack rack : repository.getAllRacks()) {
            ReplicaGroupToAssignRack result = pickReplicaGroupToAssignRack (rack, table, fracture, groups);
            LVReplicaGroup group = result.group;
            assignedGroups.add(group);
            // store the assignment to the metadata store
            repository.createNewRackAssignment(rack, fracture, group);
        }

        // The ones that got some rack need replica distribution
        for (LVReplicaGroup group : assignedGroups) {
            rebalanceReplicas (fracture, group);
        }
    }

    /** Place replica files exploiting newly assigned racks. */
    private void rebalanceReplicas (LVFracture fracture, LVReplicaGroup group) throws IOException {
        LOG.info("materializing replicas for " + group + ", fracture=" + fracture);
        // we list all the active nodes in the assigned rack(s) and the number of replica partitions they store.
        
        // TODO implement....
        for (LVRackAssignment assignment : repository.getAllRackAssignmentsByFractureId(fracture.getFractureId())) {
            if (assignment.getOwnerReplicaGroupId() == group.getGroupId()) {
                LVRack rack = repository.getRack(assignment.getRackId());
                if (rack.getStatus() == RackStatus.OK) {
                    for (LVRackNode node : repository.getAllRackNodes(rack.getRackId())) {
                        if (node.getStatus() == RackNodeStatus.OK) {
                            
                        }
                    }
                }
            }
        }
        LVSubPartitionScheme subPartitionScheme = repository.getSubPartitionSchemeByFractureAndGroup(fracture.getFractureId(), group.getGroupId());
        int rangeCount = subPartitionScheme.getRanges().length;
        for (int i = 0; i < rangeCount; ++i) {
            // repository.getReplicaPartitionCountInNode(node)
        }
    }
    @Override
    public void onLostRack(LVRack rack) throws IOException {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void onLostRackNode(LVRackNode node) throws IOException {
        // TODO Auto-generated method stub
        
    }
}
