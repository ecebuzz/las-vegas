package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.data.task.BenchmarkTpchQ1TaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * TPC-H Q1 implementation.
 * The query is so simple.
 * So this implementation works for arbitrary partitioning, sorting, and number of fractures. 
 * <pre>
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from lineitem
where l_shipdate <= date '1998-12-01' - interval ':1' day (3)
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
</pre>
 */
public class BenchmarkTpchQ1JobController extends AbstractJobController<BenchmarkTpchQ1JobParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ1JobController.class);

    public BenchmarkTpchQ1JobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ1JobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    private LVTable table;
    private static final String[] columnNames = new String[]{
        "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate"};
    private LVColumn[] columns;
    private LVReplicaScheme scheme;
    private LVFracture[] fractures;
    private LVReplica[] replicas;
    private LVReplicaPartition[] partitions;
    
    @Override
    protected void runDerived() throws IOException {
        LOG.info("going to run TPCH Q1. DELTA=" + param.getDeltaDays());
        SortedMap<Integer, ArrayList<Integer>> nodeMap = new TreeMap<Integer, ArrayList<Integer>>(); // key=nodeId. value=partition IDs
        for (LVReplicaPartition partition : partitions) {
            if (partition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                LOG.info("this partition will produce no result. skipped:" + partition);
                continue;
            }
            LOG.info("existing lineitem partition: " + partition);
            ArrayList<Integer> partitionIds = nodeMap.get(partition.getNodeId());
            if (partitionIds == null) {
                partitionIds = new ArrayList<Integer>();
                nodeMap.put (partition.getNodeId(), partitionIds);
            }
            partitionIds.add(partition.getPartitionId());
        }

        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : nodeMap.keySet()) {
            ArrayList<Integer> partitionIds = nodeMap.get(nodeId);
            
            BenchmarkTpchQ1TaskParameters taskParam = new BenchmarkTpchQ1TaskParameters();
            taskParam.setTableId(table.getTableId());
            taskParam.setDeltaDays(param.getDeltaDays());
            taskParam.setPartitionIds(ValueTraitsFactory.INTEGER_TRAITS.toArray(partitionIds));

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.BENCHMARK_TPCH_Q1, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new task to run TPCH Q1: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        LOG.info("waiting for task completion...");

        // We use a callback interface to merge sub-result _as soon as each task finishes_.
        // If we were to wait till all tasks are done and then merge all sub-results, we will most likely waste the idle CPU in the controller node.
        this.queryResult = new Q1ResultSet();
        joinTasks(taskMap, 0.01, 1.0d, new SubResultMergeCallback()); // this merges the sub-result for each finished task
        LOG.info("received all results!");
        queryResult.orderByGroup();
    }

	@Override
    protected final void initDerived() throws IOException {
        this.table = metaRepo.getTable(param.getTableId());
        assert (table != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(table.getTableId()).length != 16) {
            throw new IOException ("is this table really lineitem table? :" + table);
        }

        this.columns = new LVColumn[columnNames.length];
        for (int i = 0; i < columns.length; ++i) {
            columns[i] = metaRepo.getColumnByName(table.getTableId(), columnNames[i]);
            if (columns[i] == null) {
                throw new IOException ("column not found:" + columnNames[i]);
            }
        }
        
        this.fractures = metaRepo.getAllFractures(table.getTableId());
        
        this.scheme = metaRepo.getReplicaScheme(param.getSchemeId()); 
        assert (scheme != null);
        
        this.replicas = new LVReplica[fractures.length];
        ArrayList<LVReplicaPartition> partitionList = new ArrayList<LVReplicaPartition>();
        for (int i = 0; i < fractures.length; ++i) {
            LVFracture fracture = fractures[i];
            replicas[i] = metaRepo.getReplicaFromSchemeAndFracture(scheme.getSchemeId(), fracture.getFractureId());
            assert (replicas[i] != null);
            LVReplicaPartition[] array = metaRepo.getAllReplicaPartitionsByReplicaId(replicas[i].getReplicaId());
            for (LVReplicaPartition partition : array) {
                partitionList.add(partition);
            }
        }
        this.partitions = partitionList.toArray(new LVReplicaPartition[partitionList.size()]);
        
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q1", JobType.BENCHMARK_TPCH_Q1, null);
    }
    public static class Q1Result implements Writable {
        // these are the grouping keys of the query.
    	public String returnflag, linestatus;

    	public double quantity_sum, price_sum, discprice_sum, charge_sum, discount_sum;
    	public long count;

    	@Override
    	public String toString() {
    		return "" + returnflag + "|" + linestatus + "|" + quantity_sum + "|" + price_sum + "|" + discprice_sum + "|" + charge_sum
    		    + "|" + (quantity_sum/count) + "|" + (price_sum/count) + "|" + (discount_sum/count) + "|" + count;
    	}

    	public static Q1Result read (DataInput in) throws IOException {
    		Q1Result o = new Q1Result();
    		o.readFields(in);
    		return o;
    	}
    	@Override
    	public void readFields(DataInput in) throws IOException {
    	    returnflag = in.readUTF();
    	    linestatus = in.readUTF();
    	    quantity_sum = in.readDouble();
    	    price_sum = in.readDouble();
    	    discprice_sum = in.readDouble();
    	    charge_sum = in.readDouble();
    	    discount_sum = in.readDouble();
        	count = in.readLong();
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		out.writeUTF(returnflag);
            out.writeUTF(linestatus);
        	out.writeDouble(quantity_sum);
        	out.writeDouble(price_sum);
            out.writeDouble(discprice_sum);
            out.writeDouble(charge_sum);
            out.writeDouble(discount_sum);
            out.writeLong(count);
    	}
    }

    public static class Q1ResultSet implements Writable {
    	public ArrayList<Q1Result> tuples = new ArrayList<Q1Result>();
    	@Override
    	public String toString() {
    		StringBuffer buf = new StringBuffer(1 << 14);
            // "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate"};
			buf.append("l_returnflag|l_linestatus|SUM(l_quantity)|SUM(l_extendedprice)|SUM(l_extendedprice*(1-l_discount))|"
                + "SUM(l_extendedprice*(1-l_discount)*(1+l_tax))|AVG(l_quantity)|AVG(l_extendedprice)|AVG(l_discount)|COUNT(*)\r\n");
			buf.append("--------------------------------------------------------------------\r\n");
    		for (Q1Result tuple : tuples) {
    			buf.append(tuple);
    			buf.append("\r\n");
    		}
    		return new String(buf);
    	}
    	private Q1Result findGroup (String returnflag, String linestatus) {
            for (Q1Result tuple : tuples) { // simply sequential search. as there are few returnflag/linestatus, should be fine
                if (tuple.returnflag.equals(returnflag) && tuple.linestatus.equals(linestatus)) {
                    return tuple;
                }
            }
            // didn't find. create a new
            Q1Result group = new Q1Result();
            group.returnflag = returnflag;
            group.linestatus = linestatus;
            tuples.add(group);
            return group;
    	}
    	public void add (String returnflag, String linestatus, double quantity, double price, double discount, double tax) {
    	    // first, find the group
    	    Q1Result group = findGroup(returnflag, linestatus);

            // add it up
            group.quantity_sum += quantity;
            group.price_sum += price;
            group.discprice_sum += price * (1.0d - discount);
            group.charge_sum += price * (1.0d - discount) * (1.0d + tax);
            group.discount_sum += discount;
            ++group.count;
    	}
    	public void addAll (Q1ResultSet o) {
    		for (Q1Result tuple : o.tuples) {
                Q1Result group = findGroup(tuple.returnflag, tuple.linestatus);
                group.quantity_sum += tuple.quantity_sum;
                group.price_sum += tuple.price_sum;
                group.discprice_sum += tuple.discprice_sum;
                group.charge_sum += tuple.charge_sum;
                group.discount_sum += tuple.discount_sum;
                group.count += tuple.count;
    		}
    	}
    	public static Q1ResultSet read (DataInput in) throws IOException {
    	    Q1ResultSet o = new Q1ResultSet();
    		o.readFields(in);
    		return o;
    	}
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		int count = in.readInt();
    		tuples.clear();
    		for (int i = 0; i < count; ++i) {
    			Q1Result tuple = Q1Result.read(in);
    			tuples.add(tuple);
    		}
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		out.writeInt(tuples.size());
    		for (Q1Result tuple : tuples) {
    			tuple.write(out);
    		}
    	}
    	/** used at the end of query. re-order the result by the value of returnflag/linestatus.*/
    	public void orderByGroup () {
    	    Collections.sort(tuples, new Comparator<Q1Result>() {
    	        @Override
    	        public int compare(Q1Result o1, Q1Result o2) {
    	            {
        	            int cmp = o1.returnflag.compareTo(o2.returnflag);
        	            if (cmp != 0) {
        	                return cmp;
        	            }
    	            }
                    {
                        int cmp = o1.linestatus.compareTo(o2.linestatus);
                        if (cmp != 0) {
                            return cmp;
                        }
                    }
                    return 0;
    	        }
    	    });
    	}
    }
    
    protected Q1ResultSet queryResult;
    public final Q1ResultSet getQueryResult () {
        return queryResult;
    }
    /**
     * Called when the {@link BenchmarkTpchQ1PlanATaskRunner}
     * returns the result. Reads the sub-result file.
     */
    private class SubResultMergeCallback implements JoinTasksCallback {
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
            
    		LOG.info("reading sub-result in Node-" + nodeId + ". path=" + resultFile);
            LVRackNode node = metaRepo.getRackNode(nodeId);
            if (node == null) {
                throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
            }
    		LVDataClient client = new LVDataClient(new Configuration(), node.getAddress());
    		try {
        		VirtualFile file = new DataNodeFile(client.getChannel(), resultFile);
	        	if (!file.exists()) {
	        		throw new IOException ("sub-result file in Node-" + nodeId + " didn't exist. path=" + resultFile);
	        	}
	        	VirtualFileInputStream in = file.getInputStream();
	        	DataInputStream dataIn = new DataInputStream(in);
	        	Q1ResultSet subRanking = Q1ResultSet.read(dataIn);
	        	dataIn.close();
	        	queryResult.addAll(subRanking);
    		} finally {
				client.release();
    		}
    		LOG.info("merged one sub-result");
    	}
    }
}
