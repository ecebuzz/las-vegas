package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.FractureStatus;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.DeletePartitionFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.MergePartitionSameSchemeTaskParameters;
import edu.brown.lasvegas.lvfs.placement.PlacementEventHandlerImpl;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * The job to merge multiple fractures into one.
 */
public class MergeFractureJobController extends AbstractJobController<MergeFractureJobParameters> {
    private static Logger LOG = Logger.getLogger(MergeFractureJobController.class);
    /** existing fractures to be merged. */
    private LVFracture[] oldFractures;
    /** new fracture after merging. */
    private LVFracture newFracture;
    private LVTable table;
    private LVReplicaGroup[] groups;

    public MergeFractureJobController(LVMetadataProtocol metaRepo) throws IOException {
        super(metaRepo);
    }
    public MergeFractureJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }
    
    /** returns the newly created fracture. */
    public LVFracture getNewFracture () {
        return newFracture;
    }

    @Override
    protected void initDerived() throws IOException {
        assert (param.getFractureIds() != null);
        assert (param.getFractureIds().length >= 2);
        
        String msg = "[";
        LOG.info("merging ");
        this.oldFractures = new LVFracture[param.getFractureIds().length];
        for (int i = 0; i < oldFractures.length; ++i) {
            oldFractures[i] = metaRepo.getFracture(param.getFractureIds()[i]);
            if (oldFractures[i] == null) {
                throw new IOException ("this fracture ID doesn't exist: " + param.getFractureIds()[i]);
            }
            if (oldFractures[i].getStatus() != FractureStatus.OK) {
                throw new IOException ("this fracture isn't queriable: " + oldFractures[i]);
            }
            if (table == null) {
                table = metaRepo.getTable(oldFractures[i].getTableId());
            } else {
                if (table.getTableId() != oldFractures[i].getTableId()) {
                    throw new IOException ("this fracture belongs to a different table:" + oldFractures[i]);
                }
            }
            msg += param.getFractureIds()[i] + ",";
        }
        msg += "]";
        LOG.info("merging fractures:" + msg);
        this.jobId = metaRepo.createNewJobIdOnlyReturn("merge fractures " + msg, JobType.MERGE_FRACTURE, null);
        this.groups = metaRepo.getAllReplicaGroups(table.getTableId());
        
        this.newFracture = metaRepo.createNewFracture(table);
        assert (newFracture != null);
        // TODO to determine the placement of new fracture, we should take a look at the sizes of
        // each fracture and minimize network I/O. currently we just do the same as creating a new independent fracture.
        new PlacementEventHandlerImpl(metaRepo).onNewFracture(newFracture);
    }
    @Override
    protected void runDerived() throws IOException {
        if (!stopRequested && !errorEncountered) {
            mergePartitions(0.0d, 0.99d); // 99% of the work is this
        }
        
        if (!stopRequested && !errorEncountered) {
            deleteMergedPartitions (0.99d, 1.0d);
            
        }
        if (!stopRequested && !errorEncountered) {
            // delete metadata of merged fractures
            long totalTupleCount = 0;
            for (int i = 0; i < oldFractures.length; ++i) {
                totalTupleCount += oldFractures[i].getTupleCount();
                metaRepo.dropFracture(oldFractures[i].getFractureId());
            }
            // activate the new fracture. all done!
            metaRepo.updateFractureNoReturn(newFracture.getFractureId(), FractureStatus.OK, new LongWritable(totalTupleCount), null);
        }
    }
    
    /**
     * For each partition in the new fracture, construct the columnar files from existing
     * fractures' columnar files. When each replica group has multiple replica schemes (which should be),
     * there are multiple ways to do it.
     * 
     * Our current strategy is to be based on the same replica scheme for saving sorting overhead.
     * Because it's same replica scheme, each block shares the same sorting and we can easily
     * merge them. It's fast and simple.
     * 
     * Another possible strategy is to be based on whatever the replica scheme that has
     * corresponding columnar files in the same node, at least in the same rack.
     * This saves network I/O at the cost of sorting overhead and complexity.
     */
    private void mergePartitions (double baseProgress, double completedProgress) throws IOException {
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (LVReplicaGroup group : groups) {
            for (LVReplicaScheme scheme : metaRepo.getAllReplicaSchemes(group.getGroupId())) {
                LVReplica newReplica = metaRepo.getReplicaFromSchemeAndFracture(scheme.getSchemeId(), newFracture.getFractureId());
                LVReplicaPartition[] newPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(newReplica.getReplicaId());

                List<LVReplica> oldReplicas = new ArrayList<LVReplica>();
                List<LVReplicaPartition[]> oldReplicaPartitions = new ArrayList<LVReplicaPartition[]>();
                for (LVFracture oldFracture : oldFractures) {
                    LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(scheme.getSchemeId(), oldFracture.getFractureId());
                    oldReplicas.add(replica);
                    LVReplicaPartition[] partitions = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
                    assert(partitions.length == newPartitions.length);
                    oldReplicaPartitions.add(partitions);
                }
                
                // for each partition, create a task to merge the existing files.
                LOG.info("ReplicaScheme " + scheme + " has " + newPartitions.length + " partitions.");
                for (int i = 0; i < newPartitions.length; ++i) {
                    List<Integer> baseReplicaPartitionIds = new ArrayList<Integer>();
                    for (int j = 0; j < oldFractures.length; ++j) {
                        LVReplicaPartition partition = oldReplicaPartitions.get(j)[i];
                        if (partition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                            continue; // empty partition can be ignore while merging
                        }
                        if (partition.getStatus() != ReplicaPartitionStatus.OK) {
                            throw new IOException ("this replica partition is not queriable:" + partition);
                        }
                        baseReplicaPartitionIds.add(partition.getPartitionId());
                    }
                    if (baseReplicaPartitionIds.isEmpty()) {
                        Log.info("partition[" + i + "] has no non-empty base files.");
                        metaRepo.updateReplicaPartitionNoReturn(newPartitions[i].getPartitionId(), ReplicaPartitionStatus.EMPTY, null);
                        continue;
                    }
                    
                    int[] baseReplicaPartitionIdsArray = new int[baseReplicaPartitionIds.size()];
                    for (int j = 0; j < baseReplicaPartitionIdsArray.length; ++j) {
                        baseReplicaPartitionIdsArray[j] = baseReplicaPartitionIds.get(j);
                    }
                    
                    MergePartitionSameSchemeTaskParameters taskParam = new MergePartitionSameSchemeTaskParameters();
                    taskParam.setBasePartitionIds(baseReplicaPartitionIdsArray);
                    taskParam.setNewPartitionId(newPartitions[i].getPartitionId());
                    int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, newPartitions[i].getNodeId(), TaskType.MERGE_PARTITION_SAME_SCHEME, taskParam.writeToBytes());
                    LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
                    LOG.info("launched partition merging task: " + task);
                    assert (!taskMap.containsKey(taskId));
                    taskMap.put(taskId, task);
                }
                
            }
        }        
        joinTasks(taskMap, baseProgress, completedProgress);
    }

    private void deleteMergedPartitions (double baseProgress, double completedProgress) throws IOException {
        SortedMap<Integer, List<Integer>> basePartitionIdsToDeleteByNode = new TreeMap<Integer, List<Integer>>(); // map<nodeId, partitionIds>
        for (int i = 0; i < oldFractures.length; ++i) {
            // inactivate the merged fracture
            metaRepo.updateFractureNoReturn(oldFractures[i].getFractureId(), FractureStatus.INACTIVE, null, null);
            
            // collect target files in each node
            for (LVReplica replica : metaRepo.getAllReplicasByFractureId(oldFractures[i].getFractureId())) {
                for (LVReplicaPartition partition : metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId())) {
                    if (partition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                        continue;
                    }
                    List<Integer> ids = basePartitionIdsToDeleteByNode.get(partition.getNodeId());
                    if (ids == null) {
                        ids = new ArrayList<Integer> ();
                        basePartitionIdsToDeleteByNode.put(partition.getNodeId(), ids);
                    }
                    ids.add(partition.getPartitionId());
                }
            }
        }
        
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : basePartitionIdsToDeleteByNode.keySet()) {
            List<Integer> partitionIds = basePartitionIdsToDeleteByNode.get(nodeId);
            DeletePartitionFilesTaskParameters taskParam = new DeletePartitionFilesTaskParameters();
            int[] array = new int[partitionIds.size()];
            for (int j = 0; j < array.length; ++j) {
                array[j] = partitionIds.get(j);
            }
            taskParam.setPartitionIds(array);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.DELETE_PARTITION_FILES, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched task to delete files in " + partitionIds.size() + " partitions: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, baseProgress, completedProgress);
    }
}
