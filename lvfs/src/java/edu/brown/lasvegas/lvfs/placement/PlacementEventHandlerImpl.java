package edu.brown.lasvegas.lvfs.placement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackAssignment;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.RackStatus;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.util.IntHashSet;

/**
 * The default implementation of {@link PlacementEventHandler}.
 * This object runs on the centralized server to control all events.
 * We may want to develop a distributed version later.
 */
public final class PlacementEventHandlerImpl implements PlacementEventHandler {
    private static Logger LOG = Logger.getLogger(PlacementEventHandlerImpl.class);
    /** the metadata store. */
    private final LVMetadataProtocol repository;
    public PlacementEventHandlerImpl(LVMetadataProtocol repository) {
        this.repository = repository;
    }
    
    
    @Override
    public void onNewRack(LVRack rack) throws IOException {
        LOG.info("rebalancing for new rack:" + rack);
        for (LVDatabase database : repository.getAllDatabases()) {
            for (LVTable table : repository.getAllTables(database.getDatabaseId())) {
                LVReplicaGroup[] groups = repository.getAllReplicaGroups(table.getTableId());
                if (groups.length == 0) {
                    LOG.warn("this table doesn't have replica groups defined yet. skipped: " + table);
                    continue;
                }
                LVReplicaGroup[] linkedGroups = getLinkedGroups(groups);
                for (LVFracture fracture : repository.getAllFractures(table.getTableId())) {
                    ReplicaGroupToAssignRack result = pickReplicaGroupToAssignRack (rack, table, fracture, groups, linkedGroups);
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
    }
    private static class ReplicaGroupToAssignRack {
        LVReplicaGroup group;
        int rackCount;
    }
    /**
     * Pick the replica group that has currently the least racks assigned.
     */
    private ReplicaGroupToAssignRack pickReplicaGroupToAssignRack (LVRack rack, LVTable table, LVFracture fracture, LVReplicaGroup[] groups, LVReplicaGroup[] linkedGroups) throws IOException {
        IntHashSet otherOwnerGroupIds = new IntHashSet();
        for (LVRackAssignment assignment : repository.getAllRackAssignmentsByRackId(rack.getRackId())) {
            otherOwnerGroupIds.add(assignment.getOwnerReplicaGroupId());
        }

        HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>(); // map<groupId, count of rack assignments>
        for (int i = 0; i < groups.length; ++i) {
            LVReplicaGroup group = groups[i];
            counts.put(group.getGroupId(), 0);
            
            // linked group (co-partition) has higher priority
            if (group.getLinkedGroupId() != null && otherOwnerGroupIds.contains(group.getLinkedGroupId())) {
                ReplicaGroupToAssignRack result = new ReplicaGroupToAssignRack();
                result.group = group;
                int currentCount = 0;
                for (LVRackAssignment assignment : repository.getAllRackAssignmentsByFractureId(fracture.getFractureId())) {
                    if (assignment.getOwnerReplicaGroupId() == group.getGroupId()) {
                        ++currentCount;
                    }
                }

                result.rackCount = currentCount;
                LOG.info("picked replica group: " + group + " to assign " + rack + " because its linked group was already assigned to the rack");
            }
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

    private LVReplicaGroup[] getLinkedGroups (LVReplicaGroup[] groups) throws IOException {
        LVReplicaGroup[] linkedGroups = new LVReplicaGroup[groups.length];
        for (int i = 0; i < groups.length; ++i) {
            if (groups[i].getLinkedGroupId() != null) {
                linkedGroups[i] = repository.getReplicaGroup(groups[i].getLinkedGroupId());
            }
        }
        return linkedGroups;
    }
    
    @Override
    public void onNewFracture(LVFracture fracture) throws IOException {
        LOG.info("rebalancing for new fracture:" + fracture);
        
        // First, we assign the racks to replica groups in the table.
        LVTable table = repository.getTable(fracture.getTableId());
        LVReplicaGroup[] groups = repository.getAllReplicaGroups(fracture.getTableId());
        LVReplicaGroup[] linkedGroups = getLinkedGroups(groups);
        HashSet<LVReplicaGroup> assignedGroups = new HashSet<LVReplicaGroup>(); // groups that got some rack assignment.
        for (LVRack rack : repository.getAllRacks()) {
            ReplicaGroupToAssignRack result = pickReplicaGroupToAssignRack (rack, table, fracture, groups, linkedGroups);
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
        
        // try co-partitioning first
        if (group.getLinkedGroupId() != null) {
            LVReplicaGroup linkedGroup = repository.getReplicaGroup(group.getLinkedGroupId());
            LOG.info("co-partitioning with " + linkedGroup);
            int linkedTableId = linkedGroup.getTableId();
            LVFracture[] fractures = repository.getAllFractures(linkedTableId);
            if (fractures.length > 0) {
                // let's co-partition with its last fracture.
                LVFracture lastFracture = fractures[fractures.length - 1];
                LVReplica[] linkedReplicas = repository.getAllReplicasByFractureId(lastFracture.getFractureId());
                assert (linkedReplicas.length > 0);
                // co-partition with some of its replicas. we do round-robin here.
                // this choice is arbitrary. maybe revisit this design later

                LVReplicaScheme[] schemes = repository.getAllReplicaSchemes(group.getGroupId());
                for (int i = 0; i < schemes.length; ++i) {
                    LVReplica replica = repository.getReplicaFromSchemeAndFracture(schemes[i].getSchemeId(), fracture.getFractureId());
                    if (replica == null) {
                        replica = repository.createNewReplica(schemes[i], fracture);
                    }
                    LVReplica linkedReplica = linkedReplicas[i % (linkedReplicas.length)]; // round robin
                    LVReplicaPartition[] linkedPartitions = repository.getAllReplicaPartitionsByReplicaId(linkedReplica.getReplicaId());

                    LOG.info("co-partitioning replica " + replica + " with " + linkedReplica);
                    int partitionCount = group.getRanges().length;
                    assert (linkedPartitions.length == partitionCount);
                    for (int partition = 0; partition < partitionCount; ++partition) {
                        LVReplicaPartition replicaPartition = repository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), partition);
                        if (replicaPartition == null) {
                            replicaPartition = repository.createNewReplicaPartition(replica, partition);
                        }
                        if (linkedPartitions[partition].getNodeId() == null) {
                            throw new IOException("linked partition[" + partition + "]:" + linkedPartitions[partition]
                               + " doesn't have a node assigned");
                        }
                        repository.updateReplicaPartitionNoReturn(replicaPartition.getPartitionId(), ReplicaPartitionStatus.BEING_RECOVERED, new IntWritable(linkedPartitions[partition].getNodeId()));
                    }
                }
                
                return;
            } else {
                LOG.warn("the linked group doesn't have any fracture yet. gave up co-partitioning with " + linkedGroup);
            }
        }
        
        // we list all the active nodes in the assigned rack(s) and the number of replica partitions they store.
        HashMap<Integer, RackNodeUsage> nodeUsages = new HashMap<Integer, RackNodeUsage>();
        for (LVRackAssignment assignment : repository.getAllRackAssignmentsByFractureId(fracture.getFractureId())) {
            if (assignment.getOwnerReplicaGroupId() == group.getGroupId()) {
                LVRack rack = repository.getRack(assignment.getRackId());
                if (rack.getStatus() == RackStatus.OK) {
                    for (LVRackNode node : repository.getAllRackNodes(rack.getRackId())) {
                        assert (!nodeUsages.containsKey(node.getNodeId()));
                        if (node.getStatus() == RackNodeStatus.OK) {
                            int count = repository.getReplicaPartitionCountInNode(node);
                            nodeUsages.put(node.getNodeId(), new RackNodeUsage(node, count));
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("current node usage of " + node + ":" + count + " replica partitions");
                            }
                        }
                    }
                }
            }
        }
        
        
        // then, assign each replica partition to some node, using RackNodePriorityQueue
        RackNodePriorityQueue queue = new RackNodePriorityQueue(nodeUsages.values());
        LVReplicaScheme[] schemes = repository.getAllReplicaSchemes(group.getGroupId());
        LVReplica[] replicas = new LVReplica[schemes.length];
        for (int i = 0; i < replicas.length; ++i) {
            replicas[i] = repository.getReplicaFromSchemeAndFracture(schemes[i].getSchemeId(), fracture.getFractureId());
            if (replicas[i] == null) {
                replicas[i] = repository.createNewReplica(schemes[i], fracture);
            }
        }

        int partitionCount = group.getRanges().length;
        for (int partition = 0; partition < partitionCount; ++partition) {
            // first, check which nodes already store some replica partitions
            HashSet<Integer> usedNodeIds = new HashSet<Integer>();
            List<LVReplicaPartition> toBeStored = new ArrayList<LVReplicaPartition>();
            for (LVReplica replica : replicas) {
                LVReplicaPartition replicaPartition = repository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), partition);
                if (replicaPartition == null) {
                    replicaPartition = repository.createNewReplicaPartition(replica, partition);
                }
                
                if (replicaPartition.getNodeId() != null) {
                    // okay, this replica partition is already materialized.
                    usedNodeIds.add(replicaPartition.getNodeId());
                } else {
                    // this one needs to be stored somewhere.
                    toBeStored.add(replicaPartition);
                }
            }

            // then, assign nodes to the "toBeStored" replica partitions, avoiding the already used nodes (usedNodeIds)
            queue.moveToNextPartition(usedNodeIds);
            for (LVReplicaPartition replicaPartition : toBeStored) {
                LVRackNode node = queue.pickNode();
                repository.updateReplicaPartitionNoReturn(replicaPartition.getPartitionId(), ReplicaPartitionStatus.BEING_RECOVERED, new IntWritable(node.getNodeId()));
                if (LOG.isInfoEnabled()) {
                    LOG.info(replicaPartition + " will be materialized on " + node);
                }
            }
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
