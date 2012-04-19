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
 * This job is a slower query plan for TPC-H's Q17.
 * This doesn't assume co-partitioned part/lineitem table.
 * Thus, this job requires multiple steps as follows.
 * 
 * 1. Access lineitem table in each node, re-partition l_partkey/l_quantity/l_extprice by partkey and save the result
 * in each node.
 * 2. Again at each node, for each part partition in the node,
 * collect the re-partitioned results from all other nodes, create the
 * co-partitioned lineitem table. (equivalent to Shuffle in Hadoop)
 * 3. Then do the same as {@link BenchmarkTpchQ17JobController}.
 * 
 * I believe this is a _reasonable_ (*) query plan without partkey partitioning on lineitem.
 * And yet we will see much worse performance than partkey partitioning, which is the whole point of this experiment.
 * 
 * (*) Of course a more query-specific tuning is possible. But, shuffle-then-run is the most versatile
 * and prevalent option which will be most likely used in the real setting. So, let's compare with it.
 * @see JobType#BENCHMARK_TPCH_Q17
 */
public class BenchmarkTpchQ17PlanBJobController extends BenchmarkTpchQ17JobControllerBase {
    public BenchmarkTpchQ17PlanBJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ17PlanBJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    @Override
    protected void initDerivedPartitioning() throws IOException {
    }

    private static class NodeParam {
        List<Integer> lineitemPartitionIds = new ArrayList<Integer>();
        List<Integer> partPartitionIds = new ArrayList<Integer>();
    }
    @Override
    protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q17 with repartitioning. brand=" + param.getBrand() + ", container=" + param.getContainer());
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
