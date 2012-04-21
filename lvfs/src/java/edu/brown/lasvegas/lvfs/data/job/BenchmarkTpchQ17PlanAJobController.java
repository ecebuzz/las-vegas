package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ17TaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * This job runs TPC-H's Q17, assuming a single fracture,
 * and a co-partitioned part and lineitem table.
 * <pre>
 SELECT SUM(L_EXTENDEDPRICE) / 7 FROM LINEITEM JOIN PART ON (P_PARTKEY=L_PARTKEY)
 WHERE P_BRAND=[BRAND] AND P_CONTAINER=[CONTAINER] AND L_QUANTITY<
 (
   SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY=P_PARTKEY
 )
 </pre>
 * @see JobType#BENCHMARK_TPCH_Q17_PLANA
 */
public class BenchmarkTpchQ17PlanAJobController extends BenchmarkTpchQ17JobController {
    public BenchmarkTpchQ17PlanAJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ17PlanAJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    @Override
    protected void initDerivedTpchQ17() throws IOException {
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
    }

    private static class NodeParam {
        List<Integer> lineitemPartitionIds = new ArrayList<Integer>();
        List<Integer> partPartitionIds = new ArrayList<Integer>();
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
            
            // whether to parallelize the tasks at each node? it has tradeoffs...
            //this code runs only one task at each node
            BenchmarkTpchQ17TaskParameters taskParam = new BenchmarkTpchQ17TaskParameters();
            taskParam.setBrand(param.getBrand());
            taskParam.setContainer(param.getContainer());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setPartTableId(partTable.getTableId());
            int[] lineitemPartitionIds = asIntArray(node.lineitemPartitionIds);
            int[] partPartitionIds = asIntArray(node.partPartitionIds);
            taskParam.setLineitemPartitionIds(lineitemPartitionIds);
            taskParam.setPartPartitionIds(partPartitionIds);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q17_PLANA, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q17: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
            /*
            // this code runs one task for one partition
            for (int j = 0; j < node.lineitemPartitionIds.size(); ++j) {
                BenchmarkTpchQ17TaskParameters taskParam = new BenchmarkTpchQ17TaskParameters();
                taskParam.setBrand(param.getBrand());
                taskParam.setContainer(param.getContainer());
                taskParam.setLineitemTableId(lineitemTable.getTableId());
                taskParam.setPartTableId(partTable.getTableId());
                taskParam.setLineitemPartitionIds(new int[]{node.lineitemPartitionIds.get(j)});
                taskParam.setPartPartitionIds(new int[]{node.partPartitionIds.get(j)});
    
                int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q17, taskParam.writeToBytes());
                LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
                LOG.info("launched new task to run TPCH Q17: " + task);
                assert (!taskMap.containsKey(taskId));
                taskMap.put(taskId, task);
            }
            */
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