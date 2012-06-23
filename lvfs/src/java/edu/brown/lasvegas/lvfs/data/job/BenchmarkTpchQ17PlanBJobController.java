package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ17TaskParameters;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * This job is a slower query plan for TPC-H's Q17.
 * Part table must have only one fracture while lineitem table can have
 * an arbitrary number of fractures.
 * 
 * This doesn't assume co-partitioned part/lineitem table.
 * Thus, this job requires multiple steps as follows.
 * 
 * 1. Access lineitem table in each node, re-partition l_partkey/l_extendedprice/l_quantity by partkey and save the result
 * in each node.
 * 2. Again at each node, for each part partition in the node,
 * collect the re-partitioned results from all other nodes, create the
 * co-partitioned lineitem table. (equivalent to Shuffle in Hadoop)
 * 3. Then do the same as {@link BenchmarkTpchQ17PlanAJobController}.
 * 
 * I believe this is a _reasonable_ (*) query plan without partkey partitioning on lineitem.
 * And yet we will see much worse performance than partkey partitioning, which is the whole point of this experiment.
 * 
 * (*) Of course a more query-specific tuning is possible. But, shuffle-then-run is the most versatile
 * and prevalent option which will be most likely used in the real setting. So, let's compare with it.
 */
public class BenchmarkTpchQ17PlanBJobController extends BenchmarkTpchQ17JobController {
    public BenchmarkTpchQ17PlanBJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ17PlanBJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    private LVReplicaGroup partGroup;
    private LVColumn l_partkey, l_extendedprice, l_quantity;
    private SortedMap<Integer, ArrayList<Integer>> lineitemNodeMap;
    private SortedMap<Integer, ArrayList<Integer>> partNodeMap;
    @Override
    protected void initDerivedTpchQ17() throws IOException {
        // lineitem and part are not co-partitioned, so create node map individually

        // because we anyway do repartitioning, fractures are not an issue!
        ArrayList<LVReplicaPartition> concatenated = new ArrayList<LVReplicaPartition>();
        for (LVReplicaPartition[] partitions : lineitemPartitionLists) {
            for (LVReplicaPartition partition : partitions) {
                concatenated.add(partition);
            }
        }
        lineitemNodeMap = createNodeMap (concatenated.toArray(new LVReplicaPartition[0]), "lineitem");
        partNodeMap = createNodeMap (partPartitions, "part");
        partGroup = metaRepo.getReplicaGroup(partScheme.getGroupId());
        assert (partGroup != null);
        l_partkey = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_partkey");
        assert (l_partkey != null);
        l_extendedprice = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_extendedprice");
        assert (l_extendedprice != null);
        l_quantity = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_quantity");
        assert (l_quantity != null);
    }

    /** TODO this method should be somewhere else to share. */
    public static SortedMap<Integer, ArrayList<Integer>> createNodeMap (LVReplicaPartition[] partitions, String label) {
        SortedMap<Integer, ArrayList<Integer>> nodeMap = new TreeMap<Integer, ArrayList<Integer>>(); // key=nodeId
        for (LVReplicaPartition partition : partitions) {
            if (partition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                LOG.info("this " + label + " partition will produce no result. skipped:" + partition);
                continue;
            }
            LOG.info("existing " + label + " partition: " + partition);
            ArrayList<Integer> partitionIds = nodeMap.get(partition.getNodeId());
            if (partitionIds == null) {
            	partitionIds = new ArrayList<Integer>();
            	nodeMap.put (partition.getNodeId(), partitionIds);
            }
            partitionIds.add(partition.getPartitionId());
        }
        return nodeMap;
    }
    
    @Override
    protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q17 with repartitioning. brand=" + param.getBrand() + ", container=" + param.getContainer());

        // 1. repartition lineitem at each node.
        SortedMap<Integer, String> summaryFileMap = repartitionLineitem(0.0d, 0.5d);
        
        // 2. at each node for each part partition, collect the repartitioned lineitem files
        // and then run Q17.
        queryResult = collectAndRunQuery (summaryFileMap, 0.5d, 0.99d);
        LOG.info("all tasks including repartitioning seem done! query result=" + queryResult);
        
        // 3. delete the summary files and repartitioned files.
        SortedMap<Integer, LVTask> deleteTmpFilesTasks = RepartitionSummary.deleteRepartitionedFiles(jobId, metaRepo, summaryFileMap);
        joinTasks(deleteTmpFilesTasks, 0.99d, 1.0d);
        LOG.info("deleted temporary files");
    }
    
    private SortedMap<Integer, String> repartitionLineitem (double baseProgress, double completedProgress) throws IOException {
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : lineitemNodeMap.keySet()) {
            ArrayList<Integer> lineitemPartitionIds = lineitemNodeMap.get(nodeId);
        	RepartitionTaskParameters taskParam = new RepartitionTaskParameters();
        	taskParam.setBasePartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(lineitemPartitionIds));
        	taskParam.setOutputColumnIds(new int[]{l_partkey.getColumnId(), l_extendedprice.getColumnId(), l_quantity.getColumnId()});
        	taskParam.setOutputCompressions(new CompressionType[]{CompressionType.NONE, CompressionType.NONE, CompressionType.NONE});
        	taskParam.setPartitioningColumnId(l_partkey.getColumnId());
        	taskParam.setPartitionRanges(partGroup.getRanges());
        	taskParam.setMaxFragments(1 << 7); // at most 128 * #columns to open at once (avoid linux's no_file limit error)
            taskParam.setWriteBufferSizeTotal(1 << 27); // not too large to avoid OutofMemory.
        	taskParam.setReadCacheTuples(1 << 16);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.REPARTITION, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to repartition for TPCH Q17: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, baseProgress, completedProgress);
        return RepartitionSummary.extractSummaryFileMap(taskMap);
    }
    
    private double collectAndRunQuery (SortedMap<Integer, String> summaryFileMap, double baseProgress, double completedProgress) throws IOException {
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : partNodeMap.keySet()) {
            ArrayList<Integer> partPartitionIds = partNodeMap.get(nodeId);
            
            BenchmarkTpchQ17TaskParameters taskParam = new BenchmarkTpchQ17TaskParameters();
            taskParam.setBrand(param.getBrand());
            taskParam.setContainer(param.getContainer());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setPartTableId(partTable.getTableId());
            taskParam.setLineitemPartitionIds(new int[0]);
            taskParam.setPartPartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(partPartitionIds));
            taskParam.setRepartitionSummaryFileMap(summaryFileMap);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q17_PLANB, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q17: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, baseProgress, completedProgress);
        
        double result = 0;
        for (LVTask task : taskMap.values()) {
            String[] results = task.getOutputFilePaths();
            if (results.length != 1 && task.getStatus() == TaskStatus.DONE) {
                LOG.error("This task should be successfully done, but didn't return the result:" + task);
                continue;
            }
            result += Double.parseDouble(results[0]);
        }
    	return result;
    }
}
