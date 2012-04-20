package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;

/**
 * @see TaskType#BENCHMARK_TPCH_Q18
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

    @SuppressWarnings("unchecked")
    protected double processPartition (int partition) throws IOException {
    	LVReplicaPartition lineitemPartition = lineitemPartitions[partition];
    	LVReplicaPartition ordersPartition = ordersPartitions[partition];
    	//TODO
    	return 0;
    }
}
