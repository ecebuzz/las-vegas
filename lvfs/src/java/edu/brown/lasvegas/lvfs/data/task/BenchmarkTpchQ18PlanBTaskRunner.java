package edu.brown.lasvegas.lvfs.data.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.DataEngineContext;
import edu.brown.lasvegas.lvfs.data.PartitionMergerGeneral;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.util.MemoryUtil;

/**
 * This one collects repartitioned files and then runs the query.
 * @see TaskType#BENCHMARK_TPCH_Q17_PLANB
 */
public final class BenchmarkTpchQ18PlanBTaskRunner extends BenchmarkTpchQ18TaskRunner {
	private Map<Integer, LVColumnFile[][]> repartitionedFiles;//key=nodeId
	private Map<Integer, LVRackNode> nodeMap;//key=nodeId

    @Override
    protected void prepareInputsQ18() throws Exception {
    	/*
    	repartitionedFiles = parseSummaryFiles(context, parameters.getRepartitionSummaryFileMap());
    	nodeMap = new HashMap<Integer, LVRackNode>();
    	for (Integer nodeId : repartitionedFiles.keySet()) {
            LVRackNode node = context.metaRepo.getRackNode(nodeId);
            if (node == null) {
                throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
            }
            nodeMap.put(nodeId, node);
    	}
    	*/
    }

    @Override
    @SuppressWarnings("unchecked")
    protected double processPartition (int partition) throws IOException {
    	LVReplicaPartition ordersPartition = ordersPartitions[partition];
    	int partRange = ordersPartition.getRange();
    	//TODO
    	return 0;
    }
}
