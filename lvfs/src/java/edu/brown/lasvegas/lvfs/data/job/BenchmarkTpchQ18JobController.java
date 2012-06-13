package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.ValueIndex;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ18PlanATaskRunner;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Base class for TPC-H Q18 implementation.
 * Customer table must have only one fracture. lineitem and orders table can have
 * an arbitrary number of fractures, but the two tables must have the same number of
 * fractures with the same ranges of orderkey. This is usually true as
 * TPC-H data loading is naturally ordered by orderkey.
 * Plan A (fast query plan utilizing co-partitioning)
 * and Plan B (slow query plan using non-copartitioned files)
 * derive from this.
 * <pre>
SELECT TOP 100 C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,SUM(L_QUANTITY)
FROM LINEITEM
INNER JOIN ORDERS ON (O_ORDERKEY=L_ORDERKEY)
INNER JOIN CUSTOMER ON (C_CUSTKEY=O_CUSTKEY)
WHERE O_ORDERKEY IN (
  SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY)>[QUANTITY]
)
GROUP BY C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE
ORDER BY O_TOTALPRICE DESC, O_ORDERDATE ASC
</pre>
 */
public abstract class BenchmarkTpchQ18JobController extends AbstractJobController<BenchmarkTpchQ18JobParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ18JobController.class);

    public BenchmarkTpchQ18JobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ18JobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    protected LVTable lineitemTable, ordersTable, customerTable;
    private LVColumn c_custkey, c_name;
    protected LVReplicaScheme lineitemScheme, ordersScheme, customerScheme;
    protected LVFracture customerFracture;
    protected LVFracture lineitemFractures[], ordersFractures[]; // lineitem/orders should have same number of fractures
    protected LVReplicaPartition[] customerPartitions;
    protected LVReplicaPartition lineitemPartitionLists[][], ordersPartitionLists[][];  // first array index shared with lineitemFractures/ordersFractures
    private ValueRange[] customerRanges;
    private int[] customerStartKeys;

	@Override
    protected final void initDerived() throws IOException {
        this.lineitemTable = metaRepo.getTable(param.getLineitemTableId());
        assert (lineitemTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getLineitemTableId()).length != 16) {
            throw new IOException ("is this table really lineitem table? :" + lineitemTable);
        }

        this.ordersTable = metaRepo.getTable(param.getOrdersTableId());
        assert (ordersTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getOrdersTableId()).length != 9) {
            throw new IOException ("is this table really orders table? :" + ordersTable);
        }

        this.customerTable = metaRepo.getTable(param.getCustomerTableId());
        assert (customerTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getCustomerTableId()).length != 8) {
            throw new IOException ("is this table really customer table? :" + customerTable);
        }
        c_custkey = metaRepo.getColumnByName(customerTable.getTableId(), "c_custkey");
        assert (c_custkey != null);
        c_name = metaRepo.getColumnByName(customerTable.getTableId(), "c_name");
        assert (c_name != null);
        
        lineitemFractures = metaRepo.getAllFractures(lineitemTable.getTableId());
        ordersFractures = metaRepo.getAllFractures(ordersTable.getTableId());
        if (lineitemFractures.length != ordersFractures.length) {
            throw new IOException ("the numbers of fractures of lineitem (" + lineitemFractures.length + ") and orders (" + ordersFractures.length + ") don't match");
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(customerTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of customer table was unexpected:" + fractures.length);
            }
            customerFracture = fractures[0];
        }
        
        // TODO: so far this job assumes only one replica group and scheme in the table.
        // this should be a parameter of the job so that the caller can specify which replica scheme to use.
        // trivial to implement and so far not needed, so just a todo...
        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(lineitemTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            lineitemScheme = schemes[0];
        }

        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(ordersTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            ordersScheme = schemes[0];
        }

        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(customerTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            customerRanges = group.getRanges();
            customerScheme = schemes[0];
            customerStartKeys = (int[]) ValueRange.extractStartKeys(customerRanges);
        }
        
        LVReplica customerReplica = metaRepo.getReplicaFromSchemeAndFracture(customerScheme.getSchemeId(), customerFracture.getFractureId());
        assert (customerReplica != null);
        customerPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(customerReplica.getReplicaId());


        lineitemPartitionLists = new LVReplicaPartition[lineitemFractures.length][];
        for (int i = 0; i < lineitemFractures.length; ++i) {
            LVFracture fracture = lineitemFractures[i];
            LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), fracture.getFractureId());
            assert (replica != null);
            lineitemPartitionLists[i] = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
        }

        ordersPartitionLists = new LVReplicaPartition[ordersFractures.length][];
        for (int i = 0; i < ordersFractures.length; ++i) {
            LVFracture fracture = ordersFractures[i];
            LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(ordersScheme.getSchemeId(), fracture.getFractureId());
            assert (replica != null);
            ordersPartitionLists[i] = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
        }
        
		initDerivedTpchQ18 ();
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q18", JobType.BENCHMARK_TPCH_Q18, null);
    }
    public static class Q18Result implements Comparable<Q18Result>, Writable {
    	public String C_NAME;
    	public int C_CUSTKEY;
    	public long O_ORDERKEY;
    	public long O_ORDERDATE;
    	public double O_TOTALPRICE;
    	public double SUM_L_QUANTITY;

    	@Override
    	public String toString() {
    		return "" + C_NAME + "|" + C_CUSTKEY + "|" + O_ORDERKEY + "|" + new Date(O_ORDERDATE) + "|" + O_TOTALPRICE + "|" + SUM_L_QUANTITY;
    	}

    	/** O_TOTALPRICE DESC, O_ORDERDATE ASC, then O_ORDERKEY ASC to make it unique. */
    	@Override
    	public int compareTo(Q18Result o) {
    		// O_TOTALPRICE _DESC_
    		if (O_TOTALPRICE > o.O_TOTALPRICE) {
    			return -1;
    		} else if (O_TOTALPRICE < o.O_TOTALPRICE) {
    			return 1;
    		}
    		
    		// O_ORDERDATE ASC
    		if (O_ORDERDATE < o.O_ORDERDATE) {
    			return -1;
    		} else if (O_ORDERDATE > o.O_ORDERDATE) {
    			return 1;
    		}

    		// then O_ORDERKEY to make it unique
    		if (O_ORDERKEY < o.O_ORDERKEY) {
    			return -1;
    		} else if (O_ORDERKEY > o.O_ORDERKEY) {
    			return 1;
    		} else {
    			return 0;
    		}
    	}
    	public static Q18Result read (DataInput in) throws IOException {
    		Q18Result o = new Q18Result();
    		o.readFields(in);
    		return o;
    	}
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		C_NAME = in.readUTF();
        	C_CUSTKEY = in.readInt();
        	O_ORDERKEY = in.readLong();
        	O_ORDERDATE = in.readLong();
        	O_TOTALPRICE = in.readDouble();
        	SUM_L_QUANTITY = in.readDouble();
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		out.writeUTF(C_NAME == null ? "" : C_NAME);
        	out.writeInt(C_CUSTKEY);
        	out.writeLong(O_ORDERKEY);
        	out.writeLong(O_ORDERDATE);
        	out.writeDouble(O_TOTALPRICE);
        	out.writeDouble(SUM_L_QUANTITY);
    	}
    }

    /**
     * Results for the entire query or sub-ranking at each node. 
     */
    public static class Q18ResultRanking implements Writable {
    	private final static int TOPX = 100;
    	private Q18Result[] tuples = new Q18Result[TOPX];
    	private int count = 0;
    	
    	public int getCount () {
    		return count;
    	}
    	public ArrayList<Q18Result> getAll () {
    		assert (validate());
    		ArrayList<Q18Result> list = new ArrayList<Q18Result>();
    		for (int i = 0; i < count; ++i) {
    			list.add(tuples[i]);
    		}
    		return list;
    	}
    	@Override
    	public String toString() {
    		StringBuffer buf = new StringBuffer(1 << 14);
			buf.append("C_NAME|C_CUSTKEY|O_ORDERKEY|O_ORDERDATE|O_TOTALPRICE|SUM(L_QUANTITY)\r\n");
			buf.append("--------------------------------------------------------------------\r\n");
    		for (int i = 0; i < count; ++i) {
    			buf.append(tuples[i]);
    			buf.append("\r\n");
    		}
    		return new String(buf);
    	}
    	
    	/**
    	 * Returns if a tuple with the specified properties would be added to the ranking.
    	 * Also used to quickly prune out a tuple, which is why we don't receive Q18Result object.
    	 * Creating the object will cause overheads.
    	 */
    	public boolean willBeRanked (long orderkey, long orderdate, double totalprice) {
    		if (count < TOPX) {
    			return true; // not full yet, so anyway ok
    		}
    		Q18Result last = tuples[TOPX - 1];
    		if (totalprice > last.O_TOTALPRICE) {
    			return true;
    		} else if (totalprice < last.O_TOTALPRICE) {
    			return false;
    		}

    		if (orderdate < last.O_ORDERDATE) {
    			return true;
    		} else if (orderdate > last.O_ORDERDATE) {
    			return false;
    		}

    		if (orderkey < last.O_ORDERKEY) {
    			return true;
    		} else if (orderkey > last.O_ORDERKEY) {
    			return false;
    		}
    		assert (false); // trying to add the same orderkey? it shouldn't happen.
    		return false;
    	}
    	
    	public void add (Q18Result tuple) {
    		assert (willBeRanked(tuple.O_ORDERKEY, tuple.O_ORDERDATE, tuple.O_TOTALPRICE));
    		if (count == 0) {
    			tuples[0] = tuple;
    			++count;
    			return;
    		}
    		int index = Arrays.binarySearch(tuples, 0, count, tuple);
    		assert (index < 0); // no exact match should happen.
    		// thus, the returned value should be (-insertion_point - 1)
    		int insertionPoint = -index - 1;
    		// shift the array
    		for (int i = (count == TOPX ? TOPX - 1: count); i > insertionPoint; --i) {
    			tuples[i] = tuples[i - 1];
    		}
    		tuples[insertionPoint] = tuple;
    		if (count != TOPX) {
    			++count;
    		}
    	}
    	public void addAll (Q18ResultRanking o) {
    		for (int i = 0; i < o.count; ++i) {
        		Q18Result tuple = o.tuples[i];
        		if (willBeRanked(tuple.O_ORDERKEY, tuple.O_ORDERDATE, tuple.O_TOTALPRICE)) {
        			add (tuple);
        		}
    		}
    	}
    	/**
    	 * for debugging.
    	 * @return true if this object is in consistent state.
    	 */
    	public boolean validate () {
    		if (count > TOPX || count < 0) {
    			return false;
    		}
    		for (int i = 0; i < count; ++i) {
        		if (tuples[i] == null) {
        			return false;
        		}
    			if (i != 0 && tuples[i - 1].compareTo(tuples[i]) >= 0) {
    				return false;
    			}
    		}
    		return true;
    	}
    	public static Q18ResultRanking read (DataInput in) throws IOException {
    		Q18ResultRanking o = new Q18ResultRanking();
    		o.readFields(in);
    		return o;
    	}
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		count = in.readInt();
    		Arrays.fill(tuples, null);
    		for (int i = 0; i < count; ++i) {
    			tuples[i] = Q18Result.read(in);
    		}
    		assert (validate());
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		assert (validate());
    		out.writeInt(count);
    		for (int i = 0; i < count; ++i) {
    			tuples[i].write(out);
    		}
    	}
    }
    
    protected Q18ResultRanking queryResult;
    public final Q18ResultRanking getQueryResult () {
        return queryResult;
    }
    /**
     * use this to wait for {@link BenchmarkTpchQ18PlanATaskRunner} or {@link BenchmarkTpchQ18PlanBTaskRunner}.
     * We use a callback interface to merge sub-result _as soon as each task finishes_.
     * If we were to wait till all tasks are done and then merge all sub-results, we will most likely waste the idle CPU in the controller node.
     */
    protected final void joinQ18Tasks (SortedMap<Integer, LVTask> tasks, double baseProgress, double completedProgress) throws IOException {
    	this.queryResult = new Q18ResultRanking();
    	joinTasks(tasks, baseProgress, completedProgress, new ResultRankingMergeCallback());
    }
    /**
     * Called when the {@link BenchmarkTpchQ18PlanATaskRunner} or {@link BenchmarkTpchQ18PlanBTaskRunner}
     * returns the result. Reads the ranking file from.
     */
    private class ResultRankingMergeCallback implements JoinTasksCallback {
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
            
    		LOG.info("reading sub-ranking in Node-" + nodeId + ". path=" + resultFile);
            LVRackNode node = metaRepo.getRackNode(nodeId);
            if (node == null) {
                throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
            }
    		LVDataClient client = new LVDataClient(new Configuration(), node.getAddress());
    		try {
        		VirtualFile file = new DataNodeFile(client.getChannel(), resultFile);
	        	if (!file.exists()) {
	        		throw new IOException ("sub-ranking file in Node-" + nodeId + " didn't exist. path=" + resultFile);
	        	}
	        	VirtualFileInputStream in = file.getInputStream();
	        	DataInputStream dataIn = new DataInputStream(in);
	        	Q18ResultRanking subRanking = Q18ResultRanking.read(dataIn);
	        	dataIn.close();
	        	queryResult.addAll(subRanking);
    		} finally {
				client.release();
    		}
    		assert (queryResult.validate());
    		LOG.info("merged one ranking result");
    	}
    }
    
    /** join the result with customer to fill C_CUSTNAME. */
    protected final void fillCustomerNames () throws IOException {
		LOG.info("merged all sub-rankings. total ranking count=" + queryResult.getCount());
		assert (queryResult.validate());
    	if (queryResult.count == 0) {
    		return;
    	}
		SortedSet<Integer> custkeys = new TreeSet<Integer> ();
		for (int i = 0; i < queryResult.count; ++i) {
			Q18Result tuple = queryResult.tuples[i];
			custkeys.add(tuple.C_CUSTKEY);
		}
		// group them by customer partition
		SortedMap<Integer, ArrayList<Integer>> custRanges = new TreeMap<Integer, ArrayList<Integer>>();
		for (Integer custkey : custkeys) {
			int custRange = ValueRange.findPartition(ValueTraitsFactory.INTEGER_TRAITS, custkey, customerStartKeys);
			assert (custRange >= 0 && custRange < customerPartitions.length);
			ArrayList<Integer> keys = custRanges.get(custRange);
			if (keys == null) {
				keys = new ArrayList<Integer>();
				custRanges.put(custRange, keys);
			}
			keys.add(custkey);
		}
		
		// retrieve these customer names with multi threads.
		int threadCount;
		if (custRanges.size() < 2) {
			threadCount = 1;
		} else if (custRanges.size() < 10) {
			threadCount = 2;
		} else if (custRanges.size() < 50) {
			threadCount = 3;
		} else {
			threadCount = 4;
		}
		LOG.info("reading customer names for " + custkeys.size() + " customers from " + custRanges.size() + " partitions with " + threadCount + " threads.");
		ArrayList<CustomerNameRetrievalThread> threads = new ArrayList<CustomerNameRetrievalThread>();
		for (int i = 0; i < threadCount; ++i) {
			threads.add(new CustomerNameRetrievalThread());
		}
		allCustnames = new HashMap<Integer, String>();
	    remainingCustRetrievals = new ArrayList<CustRetrieval>();
		for (Integer custRange : custRanges.keySet()) {
			ArrayList<Integer> keysInThisRange = custRanges.get(custRange);
			assert (keysInThisRange.size() > 0);
			LVReplicaPartition customerPartition = customerPartitions[custRange];
			remainingCustRetrievals.add(new CustRetrieval(customerPartition, keysInThisRange));
		}
		// okay, ready. start!
		for (CustomerNameRetrievalThread thread : threads) {
			thread.start();
		}
		LOG.info("waiting for customer retrieval threads...");
		for (CustomerNameRetrievalThread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException ex) {
				LOG.info("waits for customer retrieval threads interrupted.", ex);
			}
		}
		LOG.info("all customer retrieval threads finished");
		for (int i = 0; i < queryResult.count; ++i) {
			Q18Result tuple = queryResult.tuples[i];
			tuple.C_NAME = allCustnames.get(tuple.C_CUSTKEY);
			assert (tuple.C_NAME != null);
		}
    }
    
    private class CustomerNameRetrievalThread extends Thread {
		Map<Integer, String> subCustnames;
    	@Override
    	public void run() {
    		try {
	    		LOG.info("customer retrieval thread " + this + " started");
	    		subCustnames = new HashMap<Integer, String>();
	    		while (true) {
	    			CustRetrieval task = getCustRetrievalTask();
	    			if (task == null) {
	    				break;
	    			}
	    			runTask(task);
	    		}
	    		addCustnames (subCustnames);
	    		LOG.info("customer retrieval thread " + this + " ended. read " + subCustnames.size() + " customers");
    		} catch (Exception ex) {
    			LOG.error("error in customer retrieval thread " + this + "!", ex);
    		}
    	}
    	@SuppressWarnings("unchecked")
		private void runTask(CustRetrieval task) throws IOException {
			ArrayList<Integer> keysInThisRange = task.customerKeys;
			assert (keysInThisRange.size() > 0);
			LVReplicaPartition customerPartition = task.customerPartition;
	    	LVColumnFile c_custkeyFile = metaRepo.getColumnFileByReplicaPartitionAndColumn(customerPartition.getPartitionId(), c_custkey.getColumnId());
			assert (c_custkeyFile != null);
			LVColumnFile c_nameFile = metaRepo.getColumnFileByReplicaPartitionAndColumn(customerPartition.getPartitionId(), c_name.getColumnId());
			assert (c_nameFile != null);
	
	        LVRackNode node = metaRepo.getRackNode(customerPartition.getNodeId());
	        if (node == null) {
	            throw new IOException ("the node ID (" + customerPartition.getNodeId() + ") doesn't exist");
	        }
			LVDataClient client = new LVDataClient(new Configuration(), node.getAddress());
			try  {
				ColumnFileBundle custkeyBundle = new ColumnFileBundle(c_custkeyFile, client.getChannel());
				ColumnFileBundle nameBundle = new ColumnFileBundle(c_nameFile, client.getChannel());
				ColumnFileReaderBundle custkeyReader = new ColumnFileReaderBundle(custkeyBundle);
				ColumnFileReaderBundle nameReader = new ColumnFileReaderBundle(nameBundle);
				ValueIndex<Integer> custkeyValueIndex = (ValueIndex<Integer>) custkeyReader.getValueIndex();
				TypedReader<String, String[]> nameDataReader = (TypedReader<String, String[]>) nameReader.getDataReader();
				nameReader.getPositionIndex(); // this loads position file for c_name
				TypedReader<Integer, int[]> custkeyDataReader = (TypedReader<Integer, int[]>) custkeyReader.getDataReader();
				// custkeys is a sorted map to speed this up. we should only seek forward
				int[] custkeyValueBuffer = new int[1 << 8];
				for (Integer custkey : keysInThisRange) {
					int custkeyInt = custkey;
					// use value index to jump to the custkey.
					// however, the index is sparse. we need to sequentially search from this position
					int tuplePos = custkeyValueIndex.searchValues(custkey);
					custkeyDataReader.seekToTupleAbsolute(tuplePos);
					
					boolean found = false;
					while (!found) {
						int read = custkeyDataReader.readValues(custkeyValueBuffer, 0, custkeyValueBuffer.length);
						if (read <= 0) {
							throw new IOException("no more tuples in customer table. couldn't find customer key: " + custkeyInt);
						}
						for (int i = 0; i < read; ++i) {
							if (custkeyValueBuffer[i] == custkeyInt) {
								found = true;
								break;
							}
							if (custkeyValueBuffer[i] > custkeyInt) {
								throw new IOException("this customer key wasn't found in customer table. foreign key violation: " + custkeyInt);
							}
							++tuplePos;
						}
					}
					
					nameDataReader.seekToTupleAbsolute(tuplePos);
					subCustnames.put(custkey, nameDataReader.readValue());
				}
				custkeyReader.close();
				nameReader.close();
			} finally {
				client.release();
			}
    	}
    }
    private ArrayList<CustRetrieval> remainingCustRetrievals;
    private Map<Integer, String> allCustnames;
    private static class CustRetrieval {
    	CustRetrieval (LVReplicaPartition customerPartition, ArrayList<Integer> customerKeys) {
    		this.customerPartition = customerPartition;
    		this.customerKeys = customerKeys;
    	}
    	LVReplicaPartition customerPartition;
    	ArrayList<Integer> customerKeys;
    }
    private CustRetrieval getCustRetrievalTask () {
    	synchronized (remainingCustRetrievals) {
    		if (remainingCustRetrievals.isEmpty()) {
    			return null;
    		}
    		return remainingCustRetrievals.remove(remainingCustRetrievals.size() - 1);
    	}
    }
    private void addCustnames (Map<Integer, String> subCustnames) {
    	synchronized (allCustnames) {
    		allCustnames.putAll(subCustnames);
		}
    }
    

    protected abstract void initDerivedTpchQ18() throws IOException;

    // TODO this function should be somewhere in shared place
    protected static int[] asIntArray (List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = list.get(i);
        }
        return array;
    }
}
