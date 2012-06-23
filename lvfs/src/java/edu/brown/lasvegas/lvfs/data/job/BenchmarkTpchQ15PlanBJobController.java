package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.ValueIndex;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15PlanBTaskRunner;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ15TaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.util.ValueRange;

/**
 * This job is a slower but general query plan for TPC-H's Q15.
 * Supplier table must have only one fracture while lineitem table can have
 * an arbitrary number of fractures.
 * 
 * This query plan collects and send all suppkey sub-aggregates
 * to calculate the global maximum TOTAL_REVENUE first. This requires to transmit
 * 10000 * ScaleFactor * (4 bytes: suppkey + 8 bytes: TOTAL_REVENUE) / SHIPDATE-selectivity.
 * Of course slower than Plan-A, but this is the best thing we can do without co-partitioning.
 */
public class BenchmarkTpchQ15PlanBJobController extends BenchmarkTpchQ15JobController {
    public BenchmarkTpchQ15PlanBJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ15PlanBJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    private SortedMap<Integer, ArrayList<Integer>> lineitemNodeMap;

    /** s_suppkey, s_name, s_address, s_phone. */
    private LVColumn[] supplierColumns;

    @Override
    protected void initDerivedTpchQ15() throws IOException {
        // because we anyway send around everything, fractures are not an issue!
        ArrayList<LVReplicaPartition> concatenated = new ArrayList<LVReplicaPartition>();
        for (LVReplicaPartition[] partitions : lineitemPartitionLists) {
            for (LVReplicaPartition partition : partitions) {
                concatenated.add(partition);
            }
        }
        lineitemNodeMap = BenchmarkTpchQ17PlanBJobController.createNodeMap (concatenated.toArray(new LVReplicaPartition[0]), "lineitem");

        String[] columnNames = new String[]{"s_suppkey", "s_name", "s_address", "s_phone"};
        supplierColumns = new LVColumn[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
        	supplierColumns[i] = metaRepo.getColumnByName(supplierTable.getTableId(), columnNames[i]);
            assert (supplierColumns[i] != null);
        }
    }

    public static class IntermediateResultSet implements Writable {
        /** key=suppkey, value=TOTAL_REVENUE .*/
        public Map<Integer, Double> results = new HashMap<Integer, Double> (1 << 16);
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		results.clear();
    		int count = in.readInt();
    		for (int i = 0; i < count; ++i) {
    			int suppkey = in.readInt();
    			double totalRevenue = in.readDouble();
    			assert (!results.containsKey(suppkey));
    			results.put(suppkey, totalRevenue);
    		}
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		final int size = results.size();
    		out.writeInt(size);
    		int count = 0;
    		for (Map.Entry<Integer, Double> entry : results.entrySet()) {
    			++count;
    			out.writeInt (entry.getKey());
    			out.writeDouble (entry.getValue());
    		}
    		assert (size == count);
    	}
    }
    private IntermediateResultSet intermediateQueryResult;
    
    @Override
    protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q15. date=" + param.getDate());
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : lineitemNodeMap.keySet()) {
            ArrayList<Integer> lineitemPartitionIds = lineitemNodeMap.get(nodeId);
            
            // whether to parallelize the tasks at each node? it has tradeoffs...
            //this code runs only one task at each node
            BenchmarkTpchQ15TaskParameters taskParam = new BenchmarkTpchQ15TaskParameters();
            taskParam.setDate(param.getDate());
            taskParam.setLineitemTableId(lineitemTable.getTableId());
            taskParam.setSupplierTableId(supplierTable.getTableId());
            taskParam.setLineitemPartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(lineitemPartitionIds));
            taskParam.setSupplierPartitionIds(new int[0]);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q15_PLANB, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q15: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        LOG.info("waiting for task completion...");
        // to speed-up "merging" the sub-aggregates, we start merging them
        // as soon as some node finishes. So, we use a callback here.
        intermediateQueryResult = new IntermediateResultSet();
        joinTasks(taskMap, 0.0d, 0.99d, new ResultMergeCallback());
        
        // now that all intermediate results are merged, we can calculate the real max_revenue
        LOG.info("got all intermediate results. checking the global max_revenue...");
        MaxSupp maxSupp = getMaxSupp();

        // then, retrieve tuples for these suppkey from supplier table.
        // we don't have to do smart things here because these are few (2-3 records might be tie, but 100-200? no way!).
        LOG.info("retrieving supplier tuples..");
        queryResult = retrieveSupplier(maxSupp);
		
        deleteTemporaryFiles (taskMap, 0.99d, 1.0d);
        LOG.info("all tasks seem done!");
    }
    private static class MaxSupp {
        double currentMaxRevenue = Double.MIN_VALUE;
        ArrayList<Integer> maxSuppkeys = new ArrayList<Integer>();
    }
    /** find the suppliers with max total revenue from intermediateQueryResult. */
    private MaxSupp getMaxSupp () {
        MaxSupp ret = new MaxSupp();
		for (Map.Entry<Integer, Double> entry : intermediateQueryResult.results.entrySet()) {
			int suppkey = entry.getKey();
			double revenue = entry.getValue();
			if (revenue == ret.currentMaxRevenue) {
				// same max revenue. add these tuples.
				ret.maxSuppkeys.add(suppkey);
			} else if (revenue > ret.currentMaxRevenue) {
				// this result updates the global max revenue.
				// so, other existing tuples are discarded.
				ret.maxSuppkeys.clear();
				ret.maxSuppkeys.add(suppkey);
				ret.currentMaxRevenue = revenue;
			} else {
	    		// then, ignored
			}
		}
        LOG.info("global max_revenue=" + ret.currentMaxRevenue + ". " + ret.maxSuppkeys.size() + " suppkeys with the total revenue");
        return ret; 
    }
	private Q15ResultList retrieveSupplier (MaxSupp maxSupp) throws IOException {
    	Q15ResultList results = new Q15ResultList();
        for (Integer suppkey : maxSupp.maxSuppkeys) {
			int suppRange = ValueRange.findPartition(ValueTraitsFactory.INTEGER_TRAITS, suppkey, supplierStartKeys);
			assert (suppRange >= 0 && suppRange < supplierPartitions.length);
			LVReplicaPartition supplierPartition = supplierPartitions[suppRange];
			
			LVRackNode node = metaRepo.getRackNode(supplierPartition.getNodeId());
	        if (node == null) {
	            throw new IOException ("the node ID (" + supplierPartition.getNodeId() + ") doesn't exist");
	        }
			LVDataClient client = new LVDataClient(new Configuration(), node.getAddress());
			try {
				results.results.add(completeQ15ResultTuple(suppkey, maxSupp.currentMaxRevenue, supplierPartition, supplierColumns, metaRepo, client));
			} finally {
				client.release();
			}
        }
        return results;
    }

    /**
     * Create Q15Result for the given supplier key by reading supplier columns.
     * @param client if the partition is remote, a client to the node. null you are sure that it's local.
     * TODO put this method somewhere else for sharing
     */
    @SuppressWarnings("unchecked")
	public static Q15Result completeQ15ResultTuple (int suppkey, double totalRevenue,
			LVReplicaPartition supplierPartition,  LVColumn[] supplierColumns,
			LVMetadataProtocol metaRepo, LVDataClient client) throws IOException {
		LVColumnFile[] columnFiles = new LVColumnFile[supplierColumns.length];
		for (int i = 0; i < supplierColumns.length; ++i) {
			columnFiles[i] = metaRepo.getColumnFileByReplicaPartitionAndColumn(supplierPartition.getPartitionId(), supplierColumns[i].getColumnId());
			assert (columnFiles[i] != null);
		}
		
		ColumnFileBundle[] bundles = new ColumnFileBundle[supplierColumns.length];
		ColumnFileReaderBundle[] readers = new ColumnFileReaderBundle[supplierColumns.length];
		for (int i = 0; i < supplierColumns.length; ++i) {
			bundles[i] = client != null ? new ColumnFileBundle(columnFiles[i], client.getChannel()) : new ColumnFileBundle(columnFiles[i]);
			readers[i] = new ColumnFileReaderBundle(bundles[i]);
		}
		ValueIndex<Integer> keyValueIndex = (ValueIndex<Integer>) readers[0].getValueIndex();
		TypedReader<Integer, int[]> keyDataReader = (TypedReader<Integer, int[]>) readers[0].getDataReader();
		TypedReader<String, String[]>[] nonKeyDataReaders = (TypedReader<String, String[]>[]) new TypedReader<?, ?>[supplierColumns.length];
		for (int i = 1; i < supplierColumns.length; ++i) {
			nonKeyDataReaders[i] = (TypedReader<String, String[]>) readers[i].getDataReader();
			readers[i].getPositionIndex(); // this loads position file
		}

		int keyInt = suppkey;
		// use value index to jump to the suppkey.
		// however, the index is sparse. we need to sequentially search from this position
		int tuplePos = keyValueIndex.searchValues(keyInt);
		keyDataReader.seekToTupleAbsolute(tuplePos);

		int[] keyValueBuffer = new int[1 << 8];
		boolean found = false;
		while (!found) {
			int read = keyDataReader.readValues(keyValueBuffer, 0, keyValueBuffer.length);
			if (read <= 0) {
				throw new IOException("no more tuples in supplier table. couldn't find supplier key: " + keyInt);
			}
			for (int i = 0; i < read; ++i) {
				if (keyValueBuffer[i] == keyInt) {
					found = true;
					break;
				}
				if (keyValueBuffer[i] > keyInt) {
					throw new IOException("this supplier key wasn't found in supplier table. foreign key violation: " + keyInt);
				}
				++tuplePos;
			}
		}

		for (int i = 1; i < supplierColumns.length; ++i) {
			nonKeyDataReaders[i].seekToTupleAbsolute(tuplePos);
		}

		Q15Result tuple = new Q15Result();
		tuple.S_SUPPKEY = suppkey;
		tuple.S_NAME = nonKeyDataReaders[1].readValue();
		tuple.S_ADDRESS = nonKeyDataReaders[2].readValue();
		tuple.S_PHONE = nonKeyDataReaders[3].readValue();
		tuple.TOTAL_REVENUE = totalRevenue;

		keyDataReader.close();
		for (int i = 1; i < supplierColumns.length; ++i) {
			nonKeyDataReaders[i].close();
		}
		return tuple;
    }

    /**
     * Called when the {@link BenchmarkTpchQ15PlanBTaskRunner}
     * returns the result. Reads the ranking file from it.
     */
    private class ResultMergeCallback implements JoinTasksCallback {
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
	        	IntermediateResultSet subResult = new IntermediateResultSet();
	        	subResult.readFields(dataIn);
	        	dataIn.close();
	        	
	        	// unlike Plan-A, each node outputs all suppkey sub-aggregates that satisfy SHIPDATE predicate.
	        	// we have to merge all of the results anyway.
	    		for (Map.Entry<Integer, Double> entry : subResult.results.entrySet()) {
	        		Double revenue = intermediateQueryResult.results.get(entry.getKey());
	        		if (revenue == null) {
	        			// new suppkey!
	        			revenue = entry.getValue();
	        		} else {
	        			// same suppkey exists. have to merge em.
	        			revenue += entry.getValue();
	        		}
	        		// update or newly put the value
        			intermediateQueryResult.results.put(entry.getKey(), entry.getValue());
	        	}
    		} finally {
				client.release();
    		}
    		LOG.info("merged one result");
    	}
    }
}
