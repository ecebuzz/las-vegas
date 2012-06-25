package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController.Q15Result;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController.Q15ResultList;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanBJobController;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * @see TaskType#BENCHMARK_TPCH_Q15_PLANC
 */
public final class BenchmarkTpchQ15PlanCTaskRunner extends BenchmarkTpchQ15TaskRunner {
    private LVTable supplier;
    /** s_suppkey, s_name, s_address, s_phone. */
    private LVColumn[] supplierColumns;
    /** num of partitions to be processed in this node. */
    protected int supplierPartitionCount;
    protected LVReplicaPartition supplierPartitions[];

    private double currentMaxRevenue;
    private ArrayList<Integer> maxSuppkeys;
    private ArrayList<Integer> maxSuppkeysPartition;

	private Map<Integer, LVColumnFile[][]> repartitionedFiles;//key=nodeId
    
    @Override
    protected final String[] runDataTask() throws Exception {
        prepareInputs ();
        long startTime = System.currentTimeMillis();
        
        // first, read all lineitems 
        currentMaxRevenue = Double.MIN_VALUE;
        maxSuppkeys = new ArrayList<Integer>();
        maxSuppkeysPartition = new ArrayList<Integer>();
        for (int i = 0; i < supplierPartitionCount; ++i) {
            LOG.info("looking for max supplier(s).. " + i + "/" + supplierPartitionCount);
            processLineitemPartition (i);
        }
        
        // then, read supplier columns for the suppkey
        LOG.info("reading supplier table for " + maxSuppkeys.size() + " tuples...");
        assert (maxSuppkeys.size() == maxSuppkeysPartition.size());
        Q15ResultList queryResult = new Q15ResultList();
        for (int i = 0; i < maxSuppkeys.size(); ++i) {
        	int suppkey = maxSuppkeys.get(i);
    		LVReplicaPartition supplierPartition = supplierPartitions[maxSuppkeysPartition.get(i)];
        	Q15Result tuple = BenchmarkTpchQ15PlanBJobController.completeQ15ResultTuple(suppkey, currentMaxRevenue,
        			supplierPartition, supplierColumns, context.metaRepo, null);
        	queryResult.add(tuple);
        }
        LOG.info("done reading supplier table.");
        
        long endTime = System.currentTimeMillis();
        LOG.info("total runDataTask() time: " + (endTime - startTime) + "ms");
        
        // serialize the result in local temporary file
        return new String[]{outputToLocalTmpFile(queryResult).getAbsolutePath()};
    }

    private void processLineitemPartition (int partition) throws IOException {
    	LVReplicaPartition supplierPartition = supplierPartitions[partition];

        Object[] mergedData = RepartitionSummary.mergeRepartitionedFilesOnMemory(
        		repartitionedFiles, context, supplierPartition.getRange(),
        			new ColumnType[]{l_suppkey.getType(), l_extendedprice.getType(), l_discount.getType(), l_shipdate.getType()}, 0);
    	if (mergedData == null) {
    		LOG.warn("no repartitioned files for this supplier partition:" + supplierPartition);
    		return;
    	}
        final int lineitemTuples = ValueTraitsFactory.INTEGER_TRAITS.length((int[]) mergedData[0]);
        LOG.info("lineitem partition tuple count=" + lineitemTuples);
        assert (ValueTraitsFactory.DOUBLE_TRAITS.length((double[]) mergedData[1]) == lineitemTuples);
        assert (ValueTraitsFactory.FLOAT_TRAITS.length((float[]) mergedData[2]) == lineitemTuples);
        assert (ValueTraitsFactory.BIGINT_TRAITS.length((long[]) mergedData[3]) == lineitemTuples);

        int[] suppkeys = (int[]) mergedData[0];
        double[] prices = (double[]) mergedData[1];
        float[] discounts = (float[]) mergedData[2];
        long[] shipdates = (long[]) mergedData[3];
        
        // because there is no fractures after repartitioning, this is even simpler than Plan-A
    	int curSuppKey = -1;
    	int curGroupCount = 0;
    	double curTotalRevenue = 0;
    	for (int i = 0; i < lineitemTuples; ++i) {
    		if (shipdates[i] < lowerShipdateValue || shipdates[i] > upperShipdateValue) {
    			continue;
    		}
    		int suppkey = suppkeys[i];
    		if (suppkey == curSuppKey) {
    			++curGroupCount;
    			curTotalRevenue += prices[i] * (1.0d - discounts[i]);
    			continue;
    		}
    		
    		assert (suppkey > curSuppKey);
    		if (curGroupCount > 0) {
    			addSubAggregate(suppkey, curTotalRevenue, partition);
    		}
    		curSuppKey = suppkey;
			curGroupCount = 1;
			curTotalRevenue = prices[i] * (1.0d - discounts[i]);
    	}
		if (curGroupCount > 0) {
			addSubAggregate(curSuppKey, curTotalRevenue, partition);
		}
    }
    private void addSubAggregate (int suppkey, double totalRevenue, int partition) {
    	if (totalRevenue == currentMaxRevenue) {
    		// tie. add this suppkey.
            maxSuppkeys.add(suppkey);
            maxSuppkeysPartition.add(partition);
    	} else if (totalRevenue > currentMaxRevenue) {
    		// new record! delete the old values;
    		maxSuppkeys.clear();
    		maxSuppkeysPartition.clear();
            maxSuppkeys.add(suppkey);
            maxSuppkeysPartition.add(partition);
            currentMaxRevenue = totalRevenue;
    	} else {
    		// then, this suppkey has no chance to be in the final query result
    	}
    }


    @Override
    protected void prepareInputsQ15() throws Exception {
        this.supplierPartitionCount = parameters.getSupplierPartitionIds().length;
        assert (supplierPartitionCount > 0);
        this.supplierPartitions = new LVReplicaPartition[supplierPartitionCount];
        for (int i = 0; i < supplierPartitionCount; ++i) {
        	supplierPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getSupplierPartitionIds()[i]);
        }

    	this.repartitionedFiles = RepartitionSummary.parseSummaryFiles(context, parameters.getRepartitionSummaryFileMap());

        this.supplier = context.metaRepo.getTable(parameters.getSupplierTableId());
        assert (supplier != null);

        String[] columnNames = new String[]{"s_suppkey", "s_name", "s_address", "s_phone"};
        this.supplierColumns = new LVColumn[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
        	supplierColumns[i] = context.metaRepo.getColumnByName(supplier.getTableId(), columnNames[i]);
            assert (supplierColumns[i] != null);
        }
    }
}
