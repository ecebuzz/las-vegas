package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.Map;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;

/**
 * This one collects repartitioned files and then runs the query.
 * @see TaskType#BENCHMARK_TPCH_Q18_PLANB
 */
public final class BenchmarkTpchQ18PlanBTaskRunner extends BenchmarkTpchQ18TaskRunner {
	private Map<Integer, LVColumnFile[][]> repartitionedFiles;//key=nodeId

    @Override
    protected void prepareInputsQ18() throws Exception {
    	repartitionedFiles = RepartitionSummary.parseSummaryFiles(context, parameters.getRepartitionSummaryFileMap());
    }

    @Override
    protected void processPartition (int partition) throws IOException {
    	LVReplicaPartition ordersPartition = ordersPartitions[partition];
    	int ordersRange = ordersPartition.getRange();

        Object[] mergedData = RepartitionSummary.mergeRepartitionedFilesOnMemory(
        		repartitionedFiles, context, ordersRange, new ColumnType[]{l_orderkey.getType(), l_quantity.getType()}, 0);
    	if (mergedData == null) {
    		LOG.warn("no repartitioned files for this part partition:" + ordersPartition);
    		return;
    	}
    	assert (mergedData.length == 2);
    	long[] lordkeys = (long[]) mergedData[0];
    	float[] quantities = (float[]) mergedData[1];
    	processPartitionCore(ordersPartition, lordkeys, quantities);
    }
}
