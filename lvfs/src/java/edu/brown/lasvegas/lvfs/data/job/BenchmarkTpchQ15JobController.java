package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.DeleteTmpFilesTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Base class for the two implementations (fast query plan and slower query plan)
 * of TPC-H Q15.
 * Supplier table must have only one fracture while lineitem table can have
 * an arbitrary number of fractures.
 * <pre>
 SELECT S_SUPPKEY,S_NAME,S_ADDRESS,S_PHONE,TOTAL_REVENUE
 FROM SUPPLIER,
 (
   SELECT L_SUPPKEY AS SUPPLIER_NO, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
   FROM LINEITEM
   WHERE L_SHIPDATE BETWEEN [DATE] AND [DATE] + 3 months
   GROUP BY L_SUPPKEY
 ) AS REVENUE
 WHERE S_SUPPKEY=SUPPLIER_NO AND TOTAL_REVENUE=
 (
   SELECT MAX(TOTAL_REVENUE) FROM REVENUE
 )
 ORDER BY S_SUPPKEY
 </pre>
 * In both implementations, this query consists of multiple stages.
 * @see JobType#BENCHMARK_TPCH_Q15
 */
public abstract class BenchmarkTpchQ15JobController extends AbstractJobController<BenchmarkTpchQ15JobParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ15JobController.class);
    
    public BenchmarkTpchQ15JobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ15JobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    protected LVTable lineitemTable, supplierTable;
    protected LVReplicaScheme lineitemScheme, supplierScheme;
    protected LVFracture supplierFracture;
    protected LVReplicaPartition[] supplierPartitions;
    protected LVFracture[] lineitemFractures;
    protected LVReplicaPartition[][] lineitemPartitionLists; // first array index shared with lineitemFractures
    protected ValueRange[] supplierRanges;
    protected int[] supplierStartKeys;

    @Override
    protected final void initDerived() throws IOException {
        this.lineitemTable = metaRepo.getTable(param.getLineitemTableId());
        assert (lineitemTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getLineitemTableId()).length != 16) {
            throw new IOException ("is this table really lineitem table? :" + lineitemTable);
        }

        this.supplierTable = metaRepo.getTable(param.getSupplierTableId());
        assert (supplierTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getSupplierTableId()).length != 7) {
            throw new IOException ("is this table really supplier table? :" + supplierTable);
        }

        lineitemFractures = metaRepo.getAllFractures(lineitemTable.getTableId());
        {
            LVFracture[] fractures = metaRepo.getAllFractures(supplierTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of supplier table was unexpected:" + fractures.length);
            }
            supplierFracture = fractures[0];
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
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(supplierTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            supplierRanges = group.getRanges();
            supplierStartKeys = (int[]) ValueRange.extractStartKeys(supplierRanges);
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            supplierScheme = schemes[0];
        }
        
        LVReplica supplierReplica = metaRepo.getReplicaFromSchemeAndFracture(supplierScheme.getSchemeId(), supplierFracture.getFractureId());
        assert (supplierReplica != null);
        supplierPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(supplierReplica.getReplicaId());

        lineitemPartitionLists = new LVReplicaPartition[lineitemFractures.length][];
        for (int i = 0; i < lineitemFractures.length; ++i) {
            LVFracture fracture = lineitemFractures[i];
            LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), fracture.getFractureId());
            assert (replica != null);
            lineitemPartitionLists[i] = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
        }
        
        initDerivedTpchQ15 ();
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q15", JobType.BENCHMARK_TPCH_Q15, param.writeToBytes());
    }
    protected abstract void initDerivedTpchQ15() throws IOException;
    
    /** one line of the query result. */
    public static class Q15Result implements Writable {
    	public int S_SUPPKEY;
    	public String S_NAME;
    	public String S_ADDRESS;
    	public String S_PHONE;
    	public double TOTAL_REVENUE;
    	@Override
    	public String toString() {
    		return "" + S_SUPPKEY + "|" + S_NAME + "|" + S_ADDRESS + "|" + S_PHONE + "|" + TOTAL_REVENUE;
    	}
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		S_SUPPKEY = in.readInt();
    		S_NAME = in.readUTF();
    		S_ADDRESS = in.readUTF();
    		S_PHONE = in.readUTF();
    		TOTAL_REVENUE = in.readDouble();
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		out.writeInt(S_SUPPKEY);
    		out.writeUTF(S_NAME);
    		out.writeUTF(S_ADDRESS);
    		out.writeUTF(S_PHONE);
    		out.writeDouble(TOTAL_REVENUE);
    	}
    }

    /** sequential list of {@link Q15Result}. */
    public static class Q15ResultList implements Writable {
    	List<Q15Result> results = new ArrayList<Q15Result>();
    	public void add (Q15Result result) {
    		results.add (result);
    	}
    	@Override
    	public void readFields(DataInput in) throws IOException {
    		results.clear();
    		int count = in.readInt();
    		for (int i = 0; i < count; ++i) {
    			Q15Result result = new Q15Result();
    			result.readFields(in);
    			results.add(result);
    		}
    	}
    	@Override
    	public void write(DataOutput out) throws IOException {
    		int count = results.size();
    		out.writeInt(count);
    		for (int i = 0; i < count; ++i) {
    			results.get(i).write(out);
    		}
    	}
    	@Override
    	public String toString() {
    		StringBuffer buf = new StringBuffer(1 << 14);
			buf.append("S_SUPPKEY|S_NAME|S_ADDRESS|S_PHONE|TOTAL_REVENUE\r\n");
			buf.append("--------------------------------------------------------------------\r\n");
    		for (Q15Result result : results) {
    			buf.append(result);
    			buf.append("\r\n");
    		}
    		return new String(buf);
    	}
    	public void clear () {
    		results.clear();
    	}
    }
    /** delete the intermediate result files generated by the given tasks. TODO this should be somewhere else to share. */
    protected void deleteTemporaryFiles (SortedMap<Integer, LVTask> taskMap, double baseProgress, double completedProgress) throws IOException {
        SortedMap<Integer, LVTask> deleteTmpFilesTasks = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : taskMap.keySet()) {
            String[] localPaths = taskMap.get(nodeId).getOutputFilePaths();
            DeleteTmpFilesTaskParameters taskParam = new DeleteTmpFilesTaskParameters();
            taskParam.setPaths(localPaths);

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.DELETE_TMP_FILES, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            assert (!deleteTmpFilesTasks.containsKey(taskId));
            deleteTmpFilesTasks.put(taskId, task);
        }
        joinTasks(deleteTmpFilesTasks, baseProgress, completedProgress);
        LOG.info("deleted temporary files");
    }
    

    protected Q15ResultList queryResult;
    public Q15ResultList getQueryResult() {
    	return queryResult;
    }

    protected static class NodeParam {
    	protected List<Integer> lineitemPartitionIds = new ArrayList<Integer>();
    	protected List<Integer> supplierPartitionIds = new ArrayList<Integer>();
    }
    protected static NodeParam getNodeParam (SortedMap<Integer, NodeParam> nodeMap, int nodeId) {
        NodeParam param = nodeMap.get(nodeId);
        if (param == null) {
            param = new NodeParam();
            nodeMap.put (nodeId, param);
        }
        return param;
    }
}
