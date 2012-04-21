package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController.Q18Result;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ18JobController.Q18ResultRanking;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * Base class for the two implementations (fast query plan and slower query plan)
 * of TPC-H Q18 Task.
 */
public abstract class BenchmarkTpchQ18TaskRunner extends DataTaskRunner<BenchmarkTpchQ18TaskParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ18TaskRunner.class);
    protected LVTable lineitem, orders;
    protected LVColumn l_orderkey, l_quantity, o_orderkey, o_custkey, o_orderdate, o_totalprice;
    /** num of partitions to be processed in this node. */
    protected int ordersPartitionCount;
    protected LVReplicaPartition ordersPartitions[];
    
    private Q18ResultRanking result;
    @Override
    protected final String[] runDataTask() throws Exception {
        prepareInputs ();
        result = new Q18ResultRanking();
        for (int i = 0; i < ordersPartitionCount; ++i) {
            LOG.info("processing.. " + i + "/" + ordersPartitionCount);
            processPartition (i);
        }
        LOG.info("sub-ranking:" + result);
        
        // serialize the sub-ranking to a file
        VirtualFile tmpFolder = new LocalVirtualFile(context.localLvfsTmpDir);
        if (!tmpFolder.exists()) {
        	tmpFolder.mkdirs();
        }
        assert (tmpFolder.exists());
        String filename = "tmp_subrank_" + Math.abs(new Random(System.nanoTime()).nextInt());
        VirtualFile subrankFile = tmpFolder.getChildFile(filename);
        VirtualFileOutputStream out = subrankFile.getOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        result.write(dataOut);
        dataOut.flush();
        dataOut.close();
        LOG.info("wrote sub-ranking to " + subrankFile.getAbsolutePath());
        return new String[]{subrankFile.getAbsolutePath()};
    }
    
    protected abstract void processPartition (int partPartition) throws IOException;

    // TODO this function should be somewhere in shared place
    protected final ColumnFileReaderBundle getReader (LVReplicaPartition partition, LVColumn column) throws IOException {
        assert (partition.getNodeId().intValue() == context.nodeId);
        LVColumnFile file = context.metaRepo.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), column.getColumnId());
        assert (file != null);
        ColumnFileBundle fileBundle = new ColumnFileBundle(file);
        return new ColumnFileReaderBundle(fileBundle, 0); // no buffering needed. we read them at once
    }
    
    // TODO this function should be somewhere in shared place
    @SuppressWarnings({ "rawtypes", "unchecked" })
	protected final Object readAtOnce (LVReplicaPartition partition, LVColumn column) throws IOException {
    	ColumnFileReaderBundle reader = getReader(partition, column);

    	int tupleCount = reader.getDataReader().getTotalTuples();
    	LOG.info("reading " + column.getName() + " at once...");
    	ValueTraits traits = ValueTraitsFactory.getInstance(column.getType());
    	Object array = traits.createArray(tupleCount);
        int read = ((TypedReader) reader.getDataReader()).readValues(array, 0, tupleCount);
        LOG.info("read.");
        reader.close();
        assert (read == tupleCount);
        
        return array;
    }

    protected final void prepareInputs () throws Exception {
        this.lineitem = context.metaRepo.getTable(parameters.getLineitemTableId());
        assert (lineitem != null);
        this.orders = context.metaRepo.getTable(parameters.getOrdersTableId());
        assert (orders != null);
        
        this.l_orderkey = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_orderkey");
        assert (l_orderkey != null);
        this.l_quantity = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_quantity");
        assert (l_quantity != null);

        this.o_orderkey = context.metaRepo.getColumnByName(orders.getTableId(), "o_orderkey");
        assert (o_orderkey != null);
        this.o_custkey = context.metaRepo.getColumnByName(orders.getTableId(), "o_custkey");
        assert (o_custkey != null);
        this.o_orderdate = context.metaRepo.getColumnByName(orders.getTableId(), "o_orderdate");
        assert (o_orderdate != null);
        this.o_totalprice = context.metaRepo.getColumnByName(orders.getTableId(), "o_totalprice");
        assert (o_totalprice != null);

        this.ordersPartitionCount = parameters.getOrdersPartitionIds().length;
        assert (ordersPartitionCount > 0);
        this.ordersPartitions = new LVReplicaPartition[ordersPartitionCount];
        for (int i = 0; i < ordersPartitionCount; ++i) {
        	ordersPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getOrdersPartitionIds()[i]);
        }
        prepareInputsQ18();
    }
    protected abstract void prepareInputsQ18 () throws Exception;

    protected final void processPartitionCore (
    		LVReplicaPartition ordersPartition,
    		long[] lordkeys, float[] quantities) throws IOException {
    	final int lineitemTuples = lordkeys.length;
    	assert (lineitemTuples == quantities.length);
    	
    	long[] oordkeys = (long[]) readAtOnce(ordersPartition, o_orderkey);
    	int[] custkeys = (int[]) readAtOnce(ordersPartition, o_custkey);
    	long[] dates = (long[]) readAtOnce(ordersPartition, o_orderdate);
    	double[] prices = (double[]) readAtOnce(ordersPartition, o_totalprice);
    	final int ordersTuples = oordkeys.length;
    	assert (ordersTuples == custkeys.length);
    	assert (ordersTuples == dates.length);
    	assert (ordersTuples == prices.length);

        int matchedOrdKeyCount = 0;
        int lineitemPos = 0;
        for (int ordersIndex = 0; ordersIndex < ordersTuples; ++ordersIndex) {
            // skip if this order will not be in the ranking anyway
            long ordkey = oordkeys[ordersIndex];
        	if (!willBeRanked(ordkey, dates[ordersIndex], prices[ordersIndex])) {
        		continue;
        	}
        	
            // find the corresponding position in lineitem
            for (; lineitemPos < lineitemTuples && lordkeys[lineitemPos] < ordkey; ++lineitemPos);
            if (lineitemPos == lineitemTuples || lordkeys[lineitemPos] > ordkey) {
                continue;
            }
        	
            // for each orderkey, first aggregate over lineitem to get the quantity sum
            assert (lordkeys[lineitemPos] == ordkey);
            double quantityTotal = 0;
            int lordEnd;
            for (lordEnd = lineitemPos; lordEnd < lineitemTuples && lordkeys[lordEnd] == ordkey; ++lordEnd) {
                quantityTotal += quantities[lordEnd];
            }
            if (quantityTotal <= parameters.getQuantityThreshold()) {
            	continue;
            }
            addRanking(custkeys[ordersIndex], ordkey, dates[ordersIndex], prices[ordersIndex], quantityTotal);

            ++matchedOrdKeyCount;
            if (LOG.isInfoEnabled() && matchedOrdKeyCount % 100 == 0) {
                LOG.info(matchedOrdKeyCount + "th matching ordkey=" + ordkey + ", oredersIndex=" + ordersIndex + ", quantityTotal=" + quantityTotal + ", lineitemPos=" + lineitemPos);
            }
            lineitemPos = lordEnd;
        }
        
        LOG.info("read the partition. in total " + matchedOrdKeyCount + " matching order key");
    }

    private boolean willBeRanked (long orderkey, long orderdate, double totalprice) {
    	return result.willBeRanked(orderkey, orderdate, totalprice);
    }
    private void addRanking (int custkey, long orderkey, long orderdate, double totalprice, double sumquantity) {
    	Q18Result tuple = new Q18Result();
    	tuple.C_CUSTKEY = custkey;
    	tuple.O_ORDERKEY = orderkey;
    	tuple.O_ORDERDATE = orderdate;
    	tuple.O_TOTALPRICE = totalprice;
    	tuple.SUM_L_QUANTITY = sumquantity;
    	result.add(tuple);
    }
    
}
