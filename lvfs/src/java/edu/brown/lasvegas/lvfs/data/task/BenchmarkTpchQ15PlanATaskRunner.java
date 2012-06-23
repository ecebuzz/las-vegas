package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController.Q15Result;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15JobController.Q15ResultList;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanBJobController;

/**
 * @see TaskType#BENCHMARK_TPCH_Q15_PLANA
 */
public final class BenchmarkTpchQ15PlanATaskRunner extends BenchmarkTpchQ15TaskRunner {
    /** key=partition range, value=corresponding lineitem partitions (one for each fracture). */
    private HashMap<Integer, ArrayList<LVReplicaPartition>> lineitemPartitions;

    private LVTable supplier;
    /** s_suppkey, s_name, s_address, s_phone. */
    private LVColumn[] supplierColumns;
    /** num of partitions to be processed in this node. */
    protected int supplierPartitionCount;
    protected LVReplicaPartition supplierPartitions[];

    private double currentMaxRevenue;
    private ArrayList<Integer> maxSuppkeys;
    private ArrayList<Integer> maxSuppkeysPartition;

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
        ArrayList<LVReplicaPartition> lineitemPartitionList = lineitemPartitions.get(supplierPartition.getRange()); // one for each fracture
        assert (lineitemPartitionList.size() > 0);

        ArrayList<LineitemFracture> lineitemFracturesList = new ArrayList<LineitemFracture>();
        for (LVReplicaPartition lineitemPartition : lineitemPartitionList) {
        	lineitemFracturesList.add(new LineitemFracture(lineitemPartition));
        }
        final LineitemFracture[] lineitemFractures = lineitemFracturesList.toArray(new LineitemFracture[0]);
        final int fractures = lineitemFractures.length;
        
        // look for the next (minimum) suppkey
        int[] posArray = new int[fractures];
        Arrays.fill(posArray, 0);
        while (true) {
        	int minSuppKey = Integer.MAX_VALUE;
        	for (int fracture = 0; fracture < fractures; ++fracture) {
        		LineitemFracture lf = lineitemFractures[fracture];
        		long shipdate = lf.shipdates[posArray[fracture]];
        		while (posArray[fracture] < lf.lineitemTuples && (shipdate < lowerShipdateValue || shipdate > upperShipdateValue)) {
        			++posArray[fracture];
        		}
        		if (posArray[fracture] == lf.lineitemTuples) {
        			continue;
        		}
        		int suppkey = lf.lsupps[posArray[fracture]];
        		if (suppkey < minSuppKey) {
        			minSuppKey = suppkey;
        		}
        	}
        	if (minSuppKey == Integer.MAX_VALUE) {
        		break;
        	}
        	
        	// then, aggregate tuples with the suppkey
        	double totalRevenue = 0;
        	for (int fracture = 0; fracture < fractures; ++fracture) {
        		LineitemFracture lf = lineitemFractures[fracture];
        		while (true) {
            		long shipdate = lf.shipdates[posArray[fracture]];
            		while (posArray[fracture] < lf.lineitemTuples && (shipdate < lowerShipdateValue || shipdate > upperShipdateValue)) {
	        			++posArray[fracture];
	        		}
            		if (posArray[fracture] == lf.lineitemTuples) {
	        			continue;
	        		}
            		int pos = posArray[fracture];
            		int suppkey = lf.lsupps[posArray[fracture]];
	        		if (suppkey == minSuppKey) {
	        			totalRevenue += lf.prices[pos] * (1.0d - lf.discounts[pos]);
	        			++posArray[fracture];
	        		} else {
	        			assert (lf.lsupps[pos] > minSuppKey);
	        			break;
	        		}
        		}
        	}
        	if (totalRevenue == currentMaxRevenue) {
        		// tie. add this suppkey.
                maxSuppkeys.add(minSuppKey);
                maxSuppkeysPartition.add(partition);
        	} else if (totalRevenue > currentMaxRevenue) {
        		// new record! delete the old values;
        		maxSuppkeys.clear();
        		maxSuppkeysPartition.clear();
                maxSuppkeys.add(minSuppKey);
                maxSuppkeysPartition.add(partition);
        	} else {
        		// then, this suppkey has no chance to be in the final query result
        	}
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

        int[] lineitemPartitionIds = parameters.getLineitemPartitionIds();
        this.lineitemPartitions = new HashMap<Integer, ArrayList<LVReplicaPartition>>();
        for (int i = 0; i < lineitemPartitionIds.length; ++i) {
            LVReplicaPartition partition = context.metaRepo.getReplicaPartition(lineitemPartitionIds[i]);
            int range = partition.getRange();
            ArrayList<LVReplicaPartition> partitions = lineitemPartitions.get(range);
            if (partitions == null) {
                partitions = new ArrayList<LVReplicaPartition>();
                lineitemPartitions.put(range, partitions);
            }
            partitions.add(partition);
        }
        
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
