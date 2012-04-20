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

/**
 * This one collects repartitioned files and then runs the query.
 * @see TaskType#BENCHMARK_TPCH_Q17_PLANB
 */
public final class BenchmarkTpchQ17PlanBTaskRunner extends BenchmarkTpchQ17PTaskRunnerBase {
	private Map<Integer, LVColumnFile[][]> repartitionedFiles;//key=nodeId
	private Map<Integer, LVRackNode> nodeMap;//key=nodeId

	/**
	 * Read repartitioning summary files and return the parsed objects.
	 * probably this function will be reused in somewhere else (esp. recovery).
	 * in that case, move this function to some shared place.
	 */
	private static Map<Integer, LVColumnFile[][]> parseSummaryFiles (
			DataEngineContext context, SortedMap<Integer, String> summaryFileMap) throws Exception {
		Map<Integer, LVColumnFile[][]> results = new HashMap<Integer, LVColumnFile[][]>();
    	for (Integer nodeId : summaryFileMap.keySet()) {
    		LVDataClient client = null;
    		try {
        		String summaryFilePath = summaryFileMap.get(nodeId);
        		LOG.info("reading repartitioned summary file in Node-" + nodeId + ". path=" + summaryFilePath);
        		VirtualFile summaryFile;
	            if (nodeId == context.nodeId) {
	                // it's in same node!
	            	summaryFile = new LocalVirtualFile(new File(summaryFilePath));
	            } else {
	                // it's remote. Connect to the node
	                LVRackNode node = context.metaRepo.getRackNode(nodeId);
	                if (node == null) {
	                    throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
	                }
	                client = new LVDataClient(context.conf, node.getAddress());
	                summaryFile = new DataNodeFile(client.getChannel(), summaryFilePath);
	            }
	        	if (!summaryFile.exists()) {
	        		throw new IOException ("repartitioned summary file in Node-" + nodeId + " didn't exist. path=" + summaryFilePath);
	        	}
	        	LVColumnFile[][] files = RepartitionTaskRunner.readSummaryFile(summaryFile);
	        	results.put(nodeId, files);
    		} finally {
    			if (client != null) {
    				client.release();
    			}
    		}
    	}
    	return results;
	}
	private Object[] getMergedData (int range, ColumnType[] columnTypes, Integer sortColumn) throws IOException {
		ArrayList<LVDataClient> clients = new ArrayList<LVDataClient>();
		try {
			ArrayList<ColumnFileBundle[]> bundles = new ArrayList<ColumnFileBundle[]>();
	    	for (Integer nodeId : repartitionedFiles.keySet()) {
	    		LVColumnFile[][] results = repartitionedFiles.get(nodeId);
	    		assert (range < results.length);
	    		LVColumnFile[] files = results[range];
	    		if (files == null) {
	    			// this node didn't have corresponding tuples, so skip it.
	    			continue;
	    		}
	    		assert (files.length == columnTypes.length);
	
	    		ColumnFileBundle[] bundle = new ColumnFileBundle[files.length];
	    		LVDataClient client = null;
	    		if (nodeId != context.nodeId) {
	                // it's remote. Connect to the node
	                LVRackNode node = nodeMap.get(nodeId);
	                assert (node != null);
	                client = new LVDataClient(context.conf, node.getAddress());
	                clients.add(client);
	    		}
	    		
	    		for (int i = 0; i < bundle.length; ++i) {
		            if (nodeId == context.nodeId) {
		            	bundle[i] = new ColumnFileBundle(files[i]);
		            } else {
		            	bundle[i] = new ColumnFileBundle(files[i], client.getChannel());
		            }
	    		}
	    		bundles.add(bundle);
	    	}
	    	if (bundles.size() == 0) {
	    		// no repartitioned files. so, the partition was empty.
	    		return null;
	    	}
	    	PartitionMergerGeneral merger = new PartitionMergerGeneral(
	    			bundles.toArray(new ColumnFileBundle[bundles.size()][]), columnTypes, sortColumn);
	    	return merger.executeOnMemory();
		} finally {
			for (LVDataClient client : clients) {
				client.release();
			}
			clients.clear();
		}
	}
	
    @Override
    protected void prepareInputsQ17() throws Exception {
    	repartitionedFiles = parseSummaryFiles(context, parameters.getRepartitionSummaryFileMap());
    	nodeMap = new HashMap<Integer, LVRackNode>();
    	for (Integer nodeId : repartitionedFiles.keySet()) {
            LVRackNode node = context.metaRepo.getRackNode(nodeId);
            if (node == null) {
                throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
            }
            nodeMap.put(nodeId, node);
    	}
    }

    @Override
    @SuppressWarnings("unchecked")
    protected double processPartition (int partition) throws IOException {
		Runtime.getRuntime().gc();
    	LVReplicaPartition partPartition = partPartitions[partition];
    	int partRange = partPartition.getRange();
    	
        ColumnFileReaderBundle p_partkeyReader = getReader(partPartition, p_partkey);
        ColumnFileReaderBundle p_brandReader = getReader(partPartition, p_brand);
        ColumnFileReaderBundle p_containerReader = getReader(partPartition, p_container);
        try {
            int partTuples = p_partkeyReader.getFileBundle().getTupleCount();
            LOG.info("part partition tuple count=" + partTuples);
            assert (p_brandReader.getFileBundle().getTupleCount() == partTuples);
            assert (p_containerReader.getFileBundle().getTupleCount() == partTuples);

            // first, check integer values that correspond to the given brand/container parameter.
            // if these are null (no corresponding value), then we even don't have to read lineitem. no result.
            Integer brandCompressed = ((OrderedDictionary<String, String[]>) p_brandReader.getDictionary()).compress(parameters.getBrand());
            assert (p_brandReader.getFileBundle().getDictionaryBytesPerEntry() == 1); // brand column has only 50 distinct values
            LOG.info("brand " + parameters.getBrand() + " corresponds to an integer value:" + brandCompressed);
            if (brandCompressed == null) {
                LOG.warn("brand " + parameters.getBrand() + " doesn't have corresponding entry in dictionary. no result.");
                return 0;
            }
            
            Integer containerCompressed = ((OrderedDictionary<String, String[]>) p_containerReader.getDictionary()).compress(parameters.getContainer());
            LOG.info("container " + parameters.getContainer() + " corresponds to an integer value:" + containerCompressed);
            if (containerCompressed == null) {
                LOG.warn("container " + parameters.getContainer() + " doesn't have corresponding entry in dictionary. no result.");
                return 0;
            }

        	Object[] mergedData = getMergedData(partRange, new ColumnType[]{l_partkey.getType(), l_extendedprice.getType(), l_quantity.getType()}, 0);
        	if (mergedData == null) {
        		LOG.warn("no repartitioned files for this part partition:" + partPartition);
        		return 0;
        	}

            int lineitemTuples = ValueTraitsFactory.INTEGER_TRAITS.length((int[]) mergedData[0]);
            LOG.info("lineitem partition tuple count=" + lineitemTuples);
            assert (ValueTraitsFactory.DOUBLE_TRAITS.length((double[]) mergedData[1]) == lineitemTuples);
            assert (ValueTraitsFactory.FLOAT_TRAITS.length((float[]) mergedData[2]) == lineitemTuples);
            return processPartitionCore(
                lineitemTuples, partTuples,
                (int[]) mergedData[0], (double[]) mergedData[1], (float[]) mergedData[2],
                (TypedReader<Integer, int[]>) p_partkeyReader.getDataReader(),
                (TypedReader<Byte, byte[]>) p_brandReader.getCompressedDataReader(), // without decompression
                p_containerReader.getCompressedDataReader(), p_containerReader.getFileBundle().getDictionaryBytesPerEntry(), // without decompression
                brandCompressed.byteValue(), containerCompressed.intValue());
        } finally {
            p_partkeyReader.close();
            p_brandReader.close();
            p_containerReader.close();
        }
    }
}
