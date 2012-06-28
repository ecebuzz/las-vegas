package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15PlanATaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15TaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * This job runs TPC-H's Q15, assuming a co-partitioned supplier and lineitem table.
 * Supplier table must have only one fracture while lineitem table can have
 * an arbitrary number of fractures.
 * 
 * To minimize network communication and disk writes, the intermediate
 * 'revenue' result is filtered by applying the L_SHIPDATE predicate and
 * the TOTAL_REVENUE=(SELECT MAX...) predicate first.
 * So, we ignore suppkey sub-aggregate whose TOTAL_REVENUE is
 * smaller than other sub-aggregate in the same node. We can safely do this optimization
 * because all related LINEITEM records of the suppkey are read together (otherwise the real TOTAL_REVENUE
 * might be different), assuming co-partitioning.
 * 
 * Then, the controller receives all of the sub-aggregate results and do the same,
 * this time resulting in the final result.
 */
public class BenchmarkTpchQ15PlanAJobController extends BenchmarkTpchQ15JobController {
    public BenchmarkTpchQ15PlanAJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ15PlanAJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    @Override
    protected void initDerivedTpchQ15() throws IOException {
        for (LVReplicaPartition[] lineitemPartitions : lineitemPartitionLists) {
            if (lineitemPartitions.length != supplierPartitions.length) {
                throw new IOException ("partition count doesn't match");
            }
        
            for (int i = 0; i < lineitemPartitions.length; ++i) {
                if (lineitemPartitions[i].getNodeId() == null) {
                    throw new IOException ("this lineitem partition doesn't have nodeId:" + lineitemPartitions[i]);
                }
                if (supplierPartitions[i].getNodeId() == null) {
                    throw new IOException ("this supplier partition doesn't have nodeId:" + supplierPartitions[i]);
                }
                if (lineitemPartitions[i].getNodeId().intValue() != supplierPartitions[i].getNodeId().intValue()) {
                    throw new IOException ("this lineitem and supplier partitions are not collocated. lineitem:" + lineitemPartitions[i] + ", supplier:" + supplierPartitions[i]);
                }
            }
        }
    }

    @Override
    protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q15. date=" + param.getDate());
        SortedMap<Integer, NodeParam> nodeMap = new TreeMap<Integer, NodeParam>(); // key=nodeId
        for (int i = 0; i < supplierPartitions.length; ++i) {
            if (supplierPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY) {
                continue;
            }
            NodeParam param = getNodeParam(nodeMap, supplierPartitions[i].getNodeId());
            param.supplierPartitionIds.add(supplierPartitions[i].getPartitionId());
        }
        // TODO this assumes all fractures co-locate a lineitem partition to supplier partition.
        // which makes all fractures share the same node coverage... not good for recoverability.
        // when we should and should not co-locate? need discussion.
        for (LVReplicaPartition[] lineitemPartitions : lineitemPartitionLists) {
            for (int i = 0; i < lineitemPartitions.length; ++i) {
                if (lineitemPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY || supplierPartitions[i].getStatus() == ReplicaPartitionStatus.EMPTY) {
                    LOG.debug("this partition will produce no result. skipped:" + lineitemPartitions[i] + "," + supplierPartitions[i]);
                    continue;
                }
                LOG.debug("existing lineitem partition: " + lineitemPartitions[i]);
                LOG.debug("existing supplier partition: " + supplierPartitions[i]);
                if (lineitemPartitions[i].getNodeId().intValue() != supplierPartitions[i].getNodeId().intValue()) {
                    throw new IOException ("not co-partitioned! lineitem partition:" + lineitemPartitions[i] + ". supplier partition:" + supplierPartitions[i]);
                }
                NodeParam param = getNodeParam(nodeMap, supplierPartitions[i].getNodeId());
                param.lineitemPartitionIds.add(lineitemPartitions[i].getPartitionId());
            }
        }

        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : nodeMap.keySet()) {
            NodeParam node = nodeMap.get(nodeId);
            
            // whether to parallelize the tasks at each node? it has tradeoffs...
            //this code runs only one task at each node
            BenchmarkTpchQ15TaskParameters taskParam = new BenchmarkTpchQ15TaskParameters();
            taskParam.setDate(param.getDate());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setSupplierTableId(supplierTable.getTableId());
            int[] lineitemPartitionIds = ValueTraitsFactory.INTEGER_TRAITS.toArray(node.lineitemPartitionIds);
            int[] supplierPartitionIds = ValueTraitsFactory.INTEGER_TRAITS.toArray(node.supplierPartitionIds);
            taskParam.setLineitemPartitionIds(lineitemPartitionIds);
            taskParam.setSupplierPartitionIds(supplierPartitionIds);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q15_PLANA, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q15: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        LOG.info("waiting for task completion...");
        // to speed-up "merging" the sub-aggregates, we start merging them
        // as soon as some node finishes. So, we use a callback here.
        Q15ResultMergeCallback callback = new Q15ResultMergeCallback(metaRepo);
        joinTasks(taskMap, 0.0d, 0.99d, callback);
        queryResult = callback.getQueryResult();

        deleteTemporaryFiles (taskMap, 0.99d, 1.0d);
        LOG.info("all tasks seem done!");
    }

    /**
     * Called when the {@link BenchmarkTpchQ15PlanATaskRunner} or {@link BenchmarkTpchQ15PlanCTaskRunner}
     * returns the result. Reads the ranking file from it.
     */
    public static class Q15ResultMergeCallback implements JoinTasksCallback {
    	public Q15ResultMergeCallback (LVMetadataProtocol metaRepo) {
    		this.metaRepo = metaRepo;
        	this.queryResult = new Q15ResultList();
            this.currentMaxRevenue = Double.MIN_VALUE;
    	}
    	private LVMetadataProtocol metaRepo;
    	private Q15ResultList queryResult;
    	public Q15ResultList getQueryResult () {return queryResult;}
        private double currentMaxRevenue;

        @Override
    	public void onTaskError(LVTask task) throws IOException {}
    	@Override
    	public void onTaskFinish(LVTask task) throws IOException {
    		int nodeId = task.getNodeId();
            String[] results = task.getOutputFilePaths();
            if (results.length != 1 && task.getStatus() == TaskStatus.DONE) {
                LOG.error("This task should be successfully done, but didn't return the result:" + task);
                return;
            }
            String resultFile = results[0];
            
    		LOG.info("reading sub-aggregate in Node-" + nodeId + ". path=" + resultFile);
            LVRackNode node = metaRepo.getRackNode(nodeId);
            if (node == null) {
                throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
            }
    		LVDataClient client = new LVDataClient(new Configuration(), node.getAddress());
    		try {
        		VirtualFile file = new DataNodeFile(client.getChannel(), resultFile);
	        	if (!file.exists()) {
	        		throw new IOException ("sub-aggregate file in Node-" + nodeId + " didn't exist. path=" + resultFile);
	        	}
	        	VirtualFileInputStream in = file.getInputStream();
	        	DataInputStream dataIn = new DataInputStream(in);
	        	Q15ResultList subResult = new Q15ResultList();
	        	subResult.readFields(dataIn);
	        	dataIn.close();
	        	
	        	// at each node, we have already applied max-revenue filtering.
	        	// so, the first tuple has the maximum total revenue at the node.
	        	if (subResult.results.size() > 0) {
	        		double revenue = subResult.results.get(0).TOTAL_REVENUE;
	        		if (revenue == currentMaxRevenue) {
	        			// same max revenue. add these tuples.
	        			queryResult.results.addAll(subResult.results);
	            		LOG.info("tie currentMaxRevenue! adding " + subResult.results.size() + " tuples");
	        		} else if (revenue > currentMaxRevenue) {
	        			// this result updates the global max revenue.
	        			// so, other existing tuples are discarded.
	        			queryResult.results = subResult.results;
	        			currentMaxRevenue = revenue;
	            		LOG.info("new max currentMaxRevenue! replacing with " + subResult.results.size() + " tuples");
	        		} else {
	        			// then, this result is ignored.
	            		LOG.info("smaller than currentMaxRevenue. ignored");
	        		}
	        	}
    		} finally {
				client.release();
    		}
    		LOG.info("merged one result");
    	}
    }
}
