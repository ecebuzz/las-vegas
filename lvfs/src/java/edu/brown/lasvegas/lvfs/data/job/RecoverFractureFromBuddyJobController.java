package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.RecoverPartitionFromBuddyTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * A job to recover all files of a replica from another replica
 * in the same group, which does NOT require repartitioning.
 * @see RecoverFractureForeignJobController
 */ 
public class RecoverFractureFromBuddyJobController extends AbstractJobController<RecoverFractureJobParameters> {
    private static Logger LOG = Logger.getLogger(RecoverFractureFromBuddyJobController.class);

    /** the concerned table. */
    private LVTable table;
    /** the concerned fracture. */
    private LVFracture fracture;
    /** the replica scheme that is damaged and to be restored. */
    private LVReplicaScheme damagedScheme;
    /** the replica scheme that is not damaged and to be used for the recovery. */
    private LVReplicaScheme sourceScheme;
    /** the group of sourceScheme and damagedScheme. */
    private LVReplicaGroup group;
    /** the concerned replicas (=fracture x scheme). */
    private LVReplica damagedReplica, sourceReplica;
    
    public RecoverFractureFromBuddyJobController(LVMetadataProtocol metaRepo) throws IOException {
        super(metaRepo);
    }
    public RecoverFractureFromBuddyJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }
    @Override
    protected void initDerived() throws IOException {
        fracture = metaRepo.getFracture(param.getFractureId());
        assert (fracture != null);
        table = metaRepo.getTable (fracture.getTableId());
        assert (table != null);
        LOG.info("recovering Fracture:" + fracture + " in Table:" + table + " from foreign replica group...");

        damagedScheme = metaRepo.getReplicaScheme(param.getDamagedSchemeId());
        assert (damagedScheme != null);
        sourceScheme = metaRepo.getReplicaScheme(param.getSourceSchemeId());
        assert (sourceScheme != null);
        LOG.info("damaged scheme:" + damagedScheme + ". intact scheme: " + sourceScheme);
        assert(sourceScheme.getGroupId() == damagedScheme.getGroupId());
        
        group = metaRepo.getReplicaGroup(damagedScheme.getGroupId());
        assert (group != null);
        assert (group.getTableId() == table.getTableId());
        LOG.info("group:" + group);
        
        damagedReplica = metaRepo.getReplicaFromSchemeAndFracture(damagedScheme.getSchemeId(), fracture.getFractureId());
        assert (damagedReplica != null);
        sourceReplica = metaRepo.getReplicaFromSchemeAndFracture(sourceScheme.getSchemeId(), fracture.getFractureId());
        assert (sourceReplica != null);
        if (sourceReplica.getStatus() != ReplicaStatus.OK) {
            throw new IOException ("the source replica is also marked as damaged. cannot recover from it. " + sourceReplica);
        }

        this.jobId = metaRepo.createNewJobIdOnlyReturn("recover Fracture:" + fracture + " in Table:" + table + " from buddy replica", JobType.RECOVER_FRACTURE_FROM_BUDDY, null);
    }
    @Override
    protected void runDerived() throws IOException {
        LVReplicaPartition[] damagedPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(damagedReplica.getReplicaId());
        SortedMap<Integer, ArrayList<Integer>> nodeMap = new TreeMap<Integer, ArrayList<Integer>>(); // key = nodeId, value = damaged partitionIDs in the node
        for (LVReplicaPartition partition : damagedPartitions) {
            if (partition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                continue; // then fine. we KNOW it will be empty again.
            }
            if (partition.getStatus() == ReplicaPartitionStatus.OK) {
                // doesn't have to be recovered.
                continue;
            }
            Integer nodeId = partition.getNodeId();
            assert (nodeId != null);
            ArrayList<Integer> partitionIds = nodeMap.get(nodeId);
            if (partitionIds == null) {
                partitionIds = new ArrayList<Integer>();
                nodeMap.put(nodeId, partitionIds);
            }
            partitionIds.add(partition.getPartitionId());
        }

        // then, create a restoration task for each of the nodes
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : nodeMap.keySet()) {
            ArrayList<Integer> partitionIds = nodeMap.get(nodeId);
            RecoverPartitionFromBuddyTaskParameters taskParam = new RecoverPartitionFromBuddyTaskParameters();
            taskParam.setPartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(partitionIds));
            taskParam.setReplicaId(damagedReplica.getReplicaId());
            taskParam.setBuddyReplicaId(sourceReplica.getReplicaId());

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.RECOVER_PARTITION_FROM_BUDDY, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to restore damaged partitions from buddy replicas: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, 0.0d, 1.0d);
        
        metaRepo.updateReplicaStatus(damagedReplica, ReplicaStatus.OK);
    }
}
