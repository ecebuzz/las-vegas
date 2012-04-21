package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;

/**
 * @see TaskType#BENCHMARK_TPCH_Q18_PLANA
 */
public final class BenchmarkTpchQ18PlanATaskRunner extends BenchmarkTpchQ18TaskRunner {
    private LVReplicaPartition lineitemPartitions[];
    @Override
    protected void prepareInputsQ18() throws Exception {
        assert (ordersPartitionCount == parameters.getLineitemPartitionIds().length);
        this.lineitemPartitions = new LVReplicaPartition[ordersPartitionCount];
        for (int i = 0; i < ordersPartitionCount; ++i) {
            lineitemPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getLineitemPartitionIds()[i]);
        }
    }

    protected void processPartition (int partition) throws IOException {
    	LVReplicaPartition lineitemPartition = lineitemPartitions[partition];
    	long[] lordkeys = (long[]) readAtOnce(lineitemPartition, l_orderkey);
    	float[] quantities = (float[]) readAtOnce(lineitemPartition, l_quantity);

    	LVReplicaPartition ordersPartition = ordersPartitions[partition];
    	processPartitionCore(ordersPartition, lordkeys, quantities);
    }
}
