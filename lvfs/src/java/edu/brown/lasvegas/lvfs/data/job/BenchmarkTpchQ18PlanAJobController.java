package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ18TaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Faster implementation of TPC-H Q18.
 * This query plan requires a co-partitioned lineitem and orders tables.
 */
public class BenchmarkTpchQ18PlanAJobController extends BenchmarkTpchQ18JobController {
    public BenchmarkTpchQ18PlanAJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ18PlanAJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }
	@Override
	protected void initDerivedTpchQ18() throws IOException {
	    assert (lineitemPartitionLists.length == ordersPartitionLists.length);
	    for (int p = 0; p < lineitemPartitionLists.length; ++p) {
	        LVReplicaPartition[] lineitemPartitions = lineitemPartitionLists[p];
            LVReplicaPartition[] ordersPartitions = ordersPartitionLists[p];
            if (lineitemPartitions.length != ordersPartitions.length) {
                throw new IOException ("partition count doesn't match");
            }
            
            for (int i = 0; i < lineitemPartitions.length; ++i) {
                if (lineitemPartitions[i].getNodeId() == null) {
                    throw new IOException ("this lineitem partition doesn't have nodeId:" + lineitemPartitions[i]);
                }
                if (ordersPartitions[i].getNodeId() == null) {
                    throw new IOException ("this orders partition doesn't have nodeId:" + ordersPartitions[i]);
                }
                if (lineitemPartitions[i].getNodeId().intValue() != ordersPartitions[i].getNodeId().intValue()) {
                    throw new IOException ("this lineitem and orders partitions are not collocated. lineitem:" + lineitemPartitions[i] + ", orders:" + ordersPartitions[i]);
                }
            }
        }
	}

	private static class NodeParam {
        List<Integer> lineitemPartitionIds = new ArrayList<Integer>();
        List<Integer> ordersPartitionIds = new ArrayList<Integer>();
    }
    private static NodeParam getNodeParam (SortedMap<Integer, NodeParam> nodeMap, int nodeId) {
        NodeParam param = nodeMap.get(nodeId);
        if (param == null) {
            param = new NodeParam();
            nodeMap.put (nodeId, param);
        }
        return param;
    }
	@Override
	protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q18. QuantityThreshold=" + param.getQuantityThreshold());
        SortedMap<Integer, NodeParam> nodeMap = new TreeMap<Integer, NodeParam>(); // key=nodeId
        for (int p = 0; p < lineitemPartitionLists.length; ++p) {
            LVReplicaPartition[] lineitemPartitions = lineitemPartitionLists[p];
            LVReplicaPartition[] ordersPartitions = ordersPartitionLists[p];
            for (int i = 0; i < lineitemPartitions.length; ++i) {
                if (lineitemPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY || ordersPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY) {
                    LOG.debug("this partition will produce no result. skipped:" + lineitemPartitions[i] + "," + ordersPartitions[i]);
                    continue;
                }
                LOG.debug("existing lineitem partition: " + lineitemPartitions[i]);
                LOG.debug("existing orders partition: " + ordersPartitions[i]);
                if (lineitemPartitions[i].getNodeId().intValue() != ordersPartitions[i].getNodeId().intValue()) {
                    throw new IOException ("not co-partitioned! lineitem partition:" + lineitemPartitions[i] + ". orders partition:" + ordersPartitions[i]);
                }
                NodeParam param = getNodeParam(nodeMap, lineitemPartitions[i].getNodeId());
                param.lineitemPartitionIds.add(lineitemPartitions[i].getPartitionId());
                param.ordersPartitionIds.add(ordersPartitions[i].getPartitionId());
            }
        }

        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : nodeMap.keySet()) {
            NodeParam node = nodeMap.get(nodeId);
            
            BenchmarkTpchQ18TaskParameters taskParam = new BenchmarkTpchQ18TaskParameters();
            taskParam.setQuantityThreshold(param.getQuantityThreshold());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setOrdersTableId(ordersTable.getTableId());
            int[] lineitemPartitionIds = asIntArray(node.lineitemPartitionIds);
            int[] ordersPartitionIds = asIntArray(node.ordersPartitionIds);
            taskParam.setLineitemPartitionIds(lineitemPartitionIds);
            taskParam.setOrdersPartitionIds(ordersPartitionIds);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q18_PLANA, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q18 Plan A: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        LOG.info("waiting for task completion...");
        joinQ18Tasks(taskMap, 0.0d, 0.9d); // this merges the sub-ranking for each finished task

        fillCustomerNames();
	}
}
