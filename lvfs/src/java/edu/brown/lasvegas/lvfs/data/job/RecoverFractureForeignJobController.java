package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
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
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;
import edu.brown.lasvegas.lvfs.data.task.RecoverPartitionFromRepartitionedFilesTaskParameters;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * A job to recover all files of a replica from another replica
 * in a different group, which requires re-partitioning.
 * 
 * This type of recovery goes as follows.
 * <ul>
 * <li>Repartition an intact replica. </li>
 * <li>In the damaged replica, collect the corresponding repartitioned files and save it in each node. </li>
 * <li>Merge and sort the repartitioned files to recover the damaged partitions. </li>
 * </ul>
 */ 
public class RecoverFractureForeignJobController extends AbstractJobController<RecoverFractureJobParameters> {
    private static Logger LOG = Logger.getLogger(RecoverFractureForeignJobController.class);

    /** the concerned table. */
    private LVTable table;
    /** columns in the table. */
    private LVColumn[] columns;
    /** and their IDs. */
    private int[] columnIds;
    /** how to compress them in the intermediate and recovered form. this is same as the compression scheme of _damaged_ scheme, not the source. */
    private CompressionType[] outputCompressions;
    /** the concerned fracture. */
    private LVFracture fracture;
    /** the replica scheme that is damaged and to be restored. */
    private LVReplicaScheme damagedScheme;
    /** the replica scheme that is not damaged and to be used for the recovery. */
    private LVReplicaScheme sourceScheme;
    /** the group of damangedScheme. */
    private LVReplicaGroup damagedGroup;
    /** the group of sourceScheme. */
    private LVReplicaGroup sourceGroup;
    /** the concerned replicas (=fracture x scheme). */
    private LVReplica damagedReplica, sourceReplica;
    private LVReplicaPartition[] sourcePartitions;
    
    public RecoverFractureForeignJobController(LVMetadataProtocol metaRepo) throws IOException {
        super(metaRepo);
    }
    public RecoverFractureForeignJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }
    @Override
    protected void initDerived() throws IOException {
        fracture = metaRepo.getFracture(param.getFractureId());
        assert (fracture != null);
        table = metaRepo.getTable (fracture.getTableId());
        assert (table != null);
        columns = metaRepo.getAllColumnsExceptEpochColumn(table.getTableId());
        columnIds = new int[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            columnIds[i] = columns[i].getColumnId();
        }
        LOG.info("recovering Fracture:" + fracture + " in Table:" + table + " from foreign replica group...");

        damagedScheme = metaRepo.getReplicaScheme(param.getDamagedSchemeId());
        assert (damagedScheme != null);
        outputCompressions = new CompressionType[columnIds.length];
        for (int i = 0; i < columns.length; ++i) {
            outputCompressions[i] = damagedScheme.getColumnCompressionScheme(columnIds[i]);
        }
        sourceScheme = metaRepo.getReplicaScheme(param.getSourceSchemeId());
        assert (sourceScheme != null);
        LOG.info("damaged scheme:" + damagedScheme + ". intact scheme: " + sourceScheme);
        assert (damagedScheme.getGroupId() != sourceScheme.getGroupId());
        
        damagedGroup = metaRepo.getReplicaGroup(damagedScheme.getGroupId());
        assert (damagedGroup != null);
        assert (damagedGroup.getTableId() == table.getTableId());
        sourceGroup = metaRepo.getReplicaGroup(sourceScheme.getGroupId());
        assert (sourceGroup != null);
        assert (sourceGroup.getTableId() == table.getTableId());
        LOG.info("damaged group:" + damagedGroup + ". intact group: " + sourceGroup);
        
        damagedReplica = metaRepo.getReplicaFromSchemeAndFracture(damagedScheme.getSchemeId(), fracture.getFractureId());
        assert (damagedReplica != null);
        sourceReplica = metaRepo.getReplicaFromSchemeAndFracture(sourceScheme.getSchemeId(), fracture.getFractureId());
        assert (sourceReplica != null);
        if (sourceReplica.getStatus() != ReplicaStatus.OK) {
            throw new IOException ("the source replica is also marked as damaged. cannot recover from it. " + sourceReplica);
        }

        sourcePartitions = metaRepo.getAllReplicaPartitionsByReplicaId(sourceReplica.getReplicaId());
        assert (sourcePartitions.length == sourceGroup.getRanges().length);

        this.jobId = metaRepo.createNewJobIdOnlyReturn("recover Fracture:" + fracture + " in Table:" + table + " from foreign replica group", JobType.RECOVER_FRACTURE_FOREIGN, null);
    }
    @Override
    protected void runDerived() throws IOException {
        // 1. repartition the intact replica at each node.
        // note that at this point we repartition everything, not skipping intact partitions in the damaged replica
        // because we might lose more nodes while doing this...
        SortedMap<Integer, String> summaryFileMap = repartitionSourceReplica(0.0d, 0.33d);
        
        // 2. copy the repartitioned files and sort/merge them into damaged partitions.
        restoreDamagedReplica(summaryFileMap, 0.33d, 0.99d);
        metaRepo.updateReplicaStatus(damagedReplica, ReplicaStatus.OK); // now it's recovered!

        // 3. delete the summary files and repartitioned files.
        SortedMap<Integer, LVTask> deleteTmpFilesTasks = RepartitionSummary.deleteRepartitionedFiles(jobId, metaRepo, summaryFileMap);
        joinTasks(deleteTmpFilesTasks, 0.99d, 1.0d);
        LOG.info("deleted temporary files");
    }

    private SortedMap<Integer, String> repartitionSourceReplica (double baseProgress, double completedProgress) throws IOException {
        // first, group source partitions by the node it resides
        SortedMap<Integer, ArrayList<Integer>> nodeMap = new TreeMap<Integer, ArrayList<Integer>>(); // key = nodeId, value = source partitionIDs in the node
        for (LVReplicaPartition partition : sourcePartitions) {
            if (partition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                continue; // this can happen and is fine
            }
            if (partition.getStatus() != ReplicaPartitionStatus.OK) {
                throw new IOException ("the source partition " + partition + " is also damaged. cannot recover from it!");
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

        // then, create a repartitioning task for each of the nodes
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : nodeMap.keySet()) {
            ArrayList<Integer> partitionIds = nodeMap.get(nodeId);
            RepartitionTaskParameters taskParam = new RepartitionTaskParameters();
            taskParam.setBasePartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(partitionIds));
            taskParam.setOutputColumnIds(columnIds);
            taskParam.setOutputCompressions(outputCompressions);
            taskParam.setPartitioningColumnId(damagedGroup.getPartitioningColumnId());
            taskParam.setPartitionRanges(damagedGroup.getRanges());
            taskParam.setMaxFragments(1 << 7); // at most 128 * #columns to open at once (avoid linux's no_file limit error)
            taskParam.setWriteBufferSizeTotal(1 << 27); // not too large to avoid OutofMemory.
            taskParam.setReadCacheTuples(1 << 16);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.REPARTITION, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to repartition for foreign recovery: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, baseProgress, completedProgress);
        return RepartitionSummary.extractSummaryFileMap(taskMap);
    }

    private void restoreDamagedReplica (SortedMap<Integer, String> summaryFileMap, double baseProgress, double completedProgress) throws IOException {
        // here we check the _updated_ status of partitions in the damaged replica.
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
            RecoverPartitionFromRepartitionedFilesTaskParameters taskParam = new RecoverPartitionFromRepartitionedFilesTaskParameters();
            taskParam.setPartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(partitionIds));
            taskParam.setReplicaId(damagedReplica.getReplicaId());
            taskParam.setRepartitionSummaryFileMap(summaryFileMap);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.RECOVER_PARTITION_FROM_REPARTITIONED_FILES, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to restore damaged partitions from repartitioned files: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, baseProgress, completedProgress);
    }
}
