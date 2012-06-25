package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanAJobController.Q15ResultMergeCallback;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15TaskParameters;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * This job is another slower but general query plan for TPC-H's Q15.
 * Supplier table must have only one fracture while lineitem table can have
 * an arbitrary number of fractures.
 * 
 * The difference from Plan-B is that this query plan first re-partitions
 * lineitem by suppkey then does the same as Plan-A.
 * Compared to Plan-B, this plan transmits most of data between each data node
 * while Plan-B receives all sub-aggregates at the central node.
 * This query plan consumes definitely smaller RAM than Plan-B which has to hold a huge
 * hash table.
 */
public class BenchmarkTpchQ15PlanCJobController extends BenchmarkTpchQ15JobController {
    public BenchmarkTpchQ15PlanCJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ15PlanCJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    private LVColumn l_suppkey, l_extendedprice, l_discount, l_shipdate;
    private SortedMap<Integer, ArrayList<Integer>> lineitemNodeMap;
    private SortedMap<Integer, ArrayList<Integer>> supplierNodeMap;

    @Override
    protected void initDerivedTpchQ15() throws IOException {
        // because we anyway repartition, fractures are not an issue!
        ArrayList<LVReplicaPartition> concatenated = new ArrayList<LVReplicaPartition>();
        for (LVReplicaPartition[] partitions : lineitemPartitionLists) {
            for (LVReplicaPartition partition : partitions) {
                concatenated.add(partition);
            }
        }
        this.lineitemNodeMap = BenchmarkTpchQ17PlanBJobController.createNodeMap (concatenated.toArray(new LVReplicaPartition[0]), "lineitem");
        this.supplierNodeMap = BenchmarkTpchQ17PlanBJobController.createNodeMap (supplierPartitions, "supplier");

        this.l_suppkey = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_suppkey");
        assert (l_suppkey != null);
        this.l_extendedprice = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_extendedprice");
        assert (l_extendedprice != null);
        this.l_discount = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_discount");
        assert (l_discount != null);
        this.l_shipdate = metaRepo.getColumnByName(lineitemTable.getTableId(), "l_shipdate");
        assert (l_shipdate != null);
    }

    @Override
    protected void runDerived() throws IOException {
    	LOG.info("going to run TPCH Q15. date=" + param.getDate());

    	// 1. repartition lineitem at each node.
        SortedMap<Integer, String> summaryFileMap = repartitionLineitem(0.0d, 0.5d);

        // 2. at each node for each supplier partition, collect the repartitioned lineitem files
        // and then run Q15.
        collectAndRunQuery (summaryFileMap, 0.5d, 0.99d);
        LOG.info("all tasks including repartitioning seem done!");
        
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
        	taskParam.setOutputColumnIds(new int[]{l_suppkey.getColumnId(), l_extendedprice.getColumnId(), l_discount.getColumnId(), l_shipdate.getColumnId()});
        	taskParam.setOutputCompressions(new CompressionType[]{CompressionType.NONE, CompressionType.NONE, CompressionType.NONE, CompressionType.NONE});
        	taskParam.setPartitioningColumnId(l_suppkey.getColumnId());
        	taskParam.setPartitionRanges(supplierRanges);
        	taskParam.setMaxFragments(1 << 7);
            taskParam.setWriteBufferSizeTotal(1 << 27);
        	taskParam.setReadCacheTuples(1 << 16);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.REPARTITION, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to repartition for TPCH Q15: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        joinTasks(taskMap, baseProgress, completedProgress);
        return RepartitionSummary.extractSummaryFileMap(taskMap);
    }

    private void collectAndRunQuery (SortedMap<Integer, String> summaryFileMap, double baseProgress, double completedProgress) throws IOException {
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : supplierNodeMap.keySet()) {
            ArrayList<Integer> supplierPartitionIds = supplierNodeMap.get(nodeId);
            
            BenchmarkTpchQ15TaskParameters taskParam = new BenchmarkTpchQ15TaskParameters();
            taskParam.setDate(param.getDate());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setSupplierTableId(supplierTable.getTableId());
            taskParam.setLineitemPartitionIds(new int[0]);
            taskParam.setSupplierPartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(supplierPartitionIds));
            taskParam.setRepartitionSummaryFileMap(summaryFileMap);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q15_PLANC, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q15: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        Q15ResultMergeCallback callback = new Q15ResultMergeCallback(metaRepo);
        joinTasks(taskMap, baseProgress, completedProgress, callback);
        queryResult = callback.getQueryResult();
    }
}
