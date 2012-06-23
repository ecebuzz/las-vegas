package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ15PlanBJobController.IntermediateResultSet;

/**
 * This one simply sends out all lineitem tuples that satisfy shipdate constraint.
 * @see TaskType#BENCHMARK_TPCH_Q15_PLANB
 */
public final class BenchmarkTpchQ15PlanBTaskRunner extends BenchmarkTpchQ15TaskRunner {
    private LVReplicaPartition[] lineitemPartitions;
    private IntermediateResultSet intermediateQueryResult;

    @Override
    protected final String[] runDataTask() throws Exception {
        prepareInputs ();
        long startTime = System.currentTimeMillis();
        
        // first, read all lineitems
        intermediateQueryResult = new IntermediateResultSet();
        int count = lineitemPartitions.length;
        for (int i = 0; i < count; ++i) {
            LOG.info("processing lineitem partitions... " + i + "/" + count);
            processLineitemPartition (lineitemPartitions[i]);
        }
        
        long endTime = System.currentTimeMillis();
        LOG.info("total runDataTask() time: " + (endTime - startTime) + "ms");
        LOG.info("distinct supplier keys from this node:" + intermediateQueryResult.results.size());
        
        // serialize the result in local temporary file
        return new String[]{outputToLocalTmpFile(intermediateQueryResult).getAbsolutePath()};
    }

    @Override
    protected void prepareInputsQ15() throws Exception {
    	lineitemPartitions = new LVReplicaPartition[parameters.getLineitemPartitionIds().length];
        for (int i = 0; i < lineitemPartitions.length; ++i) {
            int partitionId = parameters.getLineitemPartitionIds()[i];
            LVReplicaPartition lineitemPartition = context.metaRepo.getReplicaPartition(partitionId);
            if (lineitemPartition == null) {
            	throw new IOException ("this lineitem partition ID doesn't exist:" + partitionId);
            }
            if (lineitemPartition.getNodeId() != context.nodeId) {
            	throw new IOException ("this lineitem partition doesn't reside in this node:" + lineitemPartition);
            }
            if (lineitemPartition.getStatus() != ReplicaPartitionStatus.OK) {
            	throw new IOException ("this lineitem partition isn't readable:" + lineitemPartition);
            }
            lineitemPartitions[i] = lineitemPartition;
        }
    }

    private void processLineitemPartition (LVReplicaPartition lineitemPartition) throws IOException {
    	LineitemFracture fracture = new LineitemFracture(lineitemPartition);
        for (int pos = 0; pos < fracture.lineitemTuples; ++pos) {
        	long shipdate = fracture.shipdates[pos];
    		if (shipdate < lowerShipdateValue || shipdate > upperShipdateValue) {
    			continue;
    		}
    		int suppkey = fracture.lsupps[pos];
    		double price = fracture.prices[pos];
    		float discount = fracture.discounts[pos];
    		Double revenue = intermediateQueryResult.results.get(suppkey);
    		if (revenue == null) {
    			// new suppkey!
    			revenue = price * (1.0d - discount);
    		} else {
    			// same suppkey exists. have to merge em.
    			revenue += price * (1.0d - discount);
    		}
    		// update or newly put the value
			intermediateQueryResult.results.put(suppkey, revenue);
        }
    }
}
