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
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Base class for TPC-H Q18 implementation.
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
    protected LVFracture lineitemFracture, ordersFracture, customerFracture;
    protected LVReplica lineitemReplica, ordersReplica, customerReplica;
    protected LVReplicaPartition lineitemPartitions[], ordersPartitions[], customerPartition;
	private LVColumnFile c_custkeyFile, c_nameFile;

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
        
        {
            LVFracture[] fractures = metaRepo.getAllFractures(lineitemTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of lineitem table was unexpected:" + fractures.length);
            }
            lineitemFracture = fractures[0];
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(ordersTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of orders table was unexpected:" + fractures.length);
            }
            ordersFracture = fractures[0];
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(customerTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of customer table was unexpected:" + fractures.length);
            }
            customerFracture = fractures[0];
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
            customerScheme = schemes[0];
        }
        
        lineitemReplica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), lineitemFracture.getFractureId());
        assert (lineitemReplica != null);
        ordersReplica = metaRepo.getReplicaFromSchemeAndFracture(ordersScheme.getSchemeId(), ordersFracture.getFractureId());
        assert (ordersReplica != null);
        customerReplica = metaRepo.getReplicaFromSchemeAndFracture(customerScheme.getSchemeId(), customerFracture.getFractureId());
        assert (customerReplica != null);
        
        lineitemPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(lineitemReplica.getReplicaId());
        ordersPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(ordersReplica.getReplicaId());
        LVReplicaPartition[] customerPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(customerReplica.getReplicaId());
        assert (customerPartitions.length == 1);
        customerPartition = customerPartitions[0];
        
		c_custkeyFile = metaRepo.getColumnFileByReplicaPartitionAndColumn(customerPartition.getPartitionId(), c_custkey.getColumnId());
		assert (c_custkeyFile != null);
		c_nameFile = metaRepo.getColumnFileByReplicaPartitionAndColumn(customerPartition.getPartitionId(), c_name.getColumnId());
		assert (c_nameFile != null);

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
     * Read the result files from each node and calculate the final ranking from them.
     * Also, fill
     * @param completedTasks tasks that produced sub-ranking
     * @throws Exception
     */
    protected final void collectResultRanking (SortedMap<Integer, LVTask> completedTasks) throws IOException {
    	this.queryResult = new Q18ResultRanking();
    	for (LVTask task : completedTasks.values()) {
    		int nodeId = task.getNodeId();
            String[] results = task.getOutputFilePaths();
            if (results.length != 1 && task.getStatus() == TaskStatus.DONE) {
                LOG.error("This task should be successfully done, but didn't return the result:" + task);
                continue;
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
    	}
		LOG.info("merged all sub-rankings. total ranking count=" + queryResult.getCount());
		assert (queryResult.validate());
		fillCustomerNames ();
    }
    /** join the result with customer to fill C_CUSTNAME. */
    @SuppressWarnings("unchecked")
	private void fillCustomerNames () throws IOException {
    	if (queryResult.count == 0) {
    		return;
    	}
		SortedSet<Integer> custkeys = new TreeSet<Integer> ();
		for (int i = 0; i < queryResult.count; ++i) {
			Q18Result tuple = queryResult.tuples[i];
			custkeys.add(tuple.C_CUSTKEY);
		}

		LOG.info("reading customer names for " + custkeys.size() + " customers.");
		Map<Integer, String> custnames = new HashMap<Integer, String>();
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
			LOG.info("reading from 2 files...");
			int[] custkeyValueBuffer = new int[1 << 8];
			for (Integer custkey : custkeys) {
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
				custnames.put(custkey, nameDataReader.readValue());
			}
			custkeyReader.close();
			nameReader.close();
		} finally {
			client.release();
		}
		LOG.info("read customer names.");
		for (int i = 0; i < queryResult.count; ++i) {
			Q18Result tuple = queryResult.tuples[i];
			tuple.C_NAME = custnames.get(tuple.C_CUSTKEY);
			assert (tuple.C_NAME != null);
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
