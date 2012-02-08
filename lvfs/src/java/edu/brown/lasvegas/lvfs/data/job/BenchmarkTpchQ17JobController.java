package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ17TaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * This job runs TPC-H's Q17, assuming a single fracture,
 * and a co-partitioned part and lineitem table.
 * @see JobType#BENCHMARK_TPCH_Q17
 */
public class BenchmarkTpchQ17JobController extends AbstractJobController<BenchmarkTpchQ17JobParameters> {
    private static Logger LOG = Logger.getLogger(BenchmarkTpchQ17JobController.class);
    
    public BenchmarkTpchQ17JobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ17JobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    private LVTable lineitemTable, partTable;
    private LVReplicaScheme lineitemScheme, partScheme;
    private LVFracture lineitemFracture, partFracture;
    private LVReplica lineitemReplica, partReplica;
    private LVReplicaPartition lineitemPartitions[], partPartitions[];

    @Override
    protected void initDerived() throws IOException {
        this.lineitemTable = metaRepo.getTable(param.getLineitemTableId());
        assert (lineitemTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getLineitemTableId()).length != 16) {
            throw new IOException ("is this table really lineitem table? :" + lineitemTable);
        }

        this.partTable = metaRepo.getTable(param.getPartTableId());
        assert (partTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getPartTableId()).length != 9) {
            throw new IOException ("is this table really lineitem table? :" + partTable);
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(lineitemTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of lineitem table was unexpected:" + fractures.length);
            }
            lineitemFracture = fractures[0];
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(partTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of part table was unexpected:" + fractures.length);
            }
            partFracture = fractures[0];
        }
        
        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(lineitemTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            lineitemScheme = schemes[0];
        }

        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(partTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            partScheme = schemes[0];
        }
        
        lineitemReplica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), lineitemFracture.getFractureId());
        assert (lineitemReplica != null);
        partReplica = metaRepo.getReplicaFromSchemeAndFracture(partScheme.getSchemeId(), partFracture.getFractureId());
        assert (partReplica != null);
        
        lineitemPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(lineitemReplica.getReplicaId());
        partPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(partReplica.getReplicaId());
        
        if (lineitemPartitions.length != partPartitions.length) {
            throw new IOException ("partition count doesn't match");
        }
        
        for (int i = 0; i < lineitemPartitions.length; ++i) {
            if (lineitemPartitions[i].getNodeId() == null) {
                throw new IOException ("this lineitem partition doesn't have nodeId:" + lineitemPartitions[i]);
            }
            if (partPartitions[i].getNodeId() == null) {
                throw new IOException ("this part partition doesn't have nodeId:" + partPartitions[i]);
            }
            if (lineitemPartitions[i].getNodeId().intValue() != partPartitions[i].getNodeId().intValue()) {
                throw new IOException ("this lineitem and part partitions are not collocated. lineitem:" + lineitemPartitions[i] + ", part:" + partPartitions[i]);
            }
        }
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q17", JobType.BENCHMARK_TPCH_Q17, null);
    }
    
    private double queryResult = 0;
    public double getQueryResult () {
        return queryResult;
    }

    private static class NodeParam {
        List<Integer> lineitemPartitionIds = new ArrayList<Integer>();
        List<Integer> partPartitionIds = new ArrayList<Integer>();
    }
    private static int[] asIntArray (List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = list.get(i);
        }
        return array;
    }
    @Override
    protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q17. brand=" + param.getBrand() + ", container=" + param.getContainer());
        SortedMap<Integer, NodeParam> nodeMap = new TreeMap<Integer, NodeParam>(); // key=nodeId
        for (int i = 0; i < lineitemPartitions.length; ++i) {
            if (lineitemPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY || partPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY) {
                LOG.info("this partition will produce no result. skipped:" + lineitemPartitions[i] + "," + partPartitions[i]);
                continue;
            }
            LOG.info("existing lineitem partition: " + lineitemPartitions[i]);
            LOG.info("existing part partition: " + partPartitions[i]);
            if (lineitemPartitions[i].getNodeId().intValue() != partPartitions[i].getNodeId().intValue()) {
                throw new IOException ("not co-partitioned! lineitem partition:" + lineitemPartitions[i] + ". part partition:" + partPartitions[i]);
            }
            NodeParam param = nodeMap.get(lineitemPartitions[i].getNodeId());
            if (param == null) {
                param = new NodeParam();
                nodeMap.put (lineitemPartitions[i].getNodeId(), param);
            }
            param.lineitemPartitionIds.add(lineitemPartitions[i].getPartitionId());
            param.partPartitionIds.add(partPartitions[i].getPartitionId());
        }

        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : nodeMap.keySet()) {
            NodeParam node = nodeMap.get(nodeId);
            BenchmarkTpchQ17TaskParameters taskParam = new BenchmarkTpchQ17TaskParameters();
            taskParam.setBrand(param.getBrand());
            taskParam.setContainer(param.getContainer());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setPartTableId(partTable.getTableId());
            
            int[] lineitemPartitionIds = asIntArray(node.lineitemPartitionIds);
            int[] partPartitionIds = asIntArray(node.partPartitionIds);
            taskParam.setLineitemPartitionIds(lineitemPartitionIds);
            taskParam.setPartPartitionIds(partPartitionIds);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q17, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q17: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        LOG.info("waiting for task completion...");
        joinTasks(taskMap, 0.0d, 1.0d);
        
        LOG.info("all tasks seem done!");
        queryResult = 0;
        for (LVTask task : metaRepo.getAllTasksByJob(jobId)) {
            // a hack. see BenchmarkTpchQ17TaskRunner. this property is used to store the subtotal from the node. 
            String[] results = task.getOutputFilePaths();
            if (results.length != 1 && task.getStatus() == TaskStatus.DONE) {
                LOG.error("This task should be successfully done, but didn't return the result:" + task);
                continue;
            }
            queryResult += Double.parseDouble(results[0]);
        }
        LOG.info("query result=" + queryResult);
    }
}
