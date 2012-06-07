package edu.brown.lasvegas.lvfs.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;
import edu.brown.lasvegas.lvfs.data.task.DeleteTmpFilesTaskParameters;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Some static protocol about the summary files for {@link Repartitioner}.
 */
public final class RepartitionSummary {
	private RepartitionSummary(){}
    private static Logger LOG = Logger.getLogger(RepartitionSummary.class);
	/**
	 * Serializes the location/metadata/etc of repartitioned files in a summary file.
	 * @param outputFolder the folder to place summary file.
	 * @param result result of repartitioning ({@link Repartitioner})
	 * @return path of the summary file
	 */
    public static String createSummaryFile(VirtualFile outputFolder, LVColumnFile[][] result) throws Exception {
    	VirtualFile summaryFile = outputFolder.getChildFile("summary.bin");
    	if (summaryFile.exists()) {
    		throw new IOException (summaryFile.getAbsolutePath() + " already exists. probably an incorrect output folder specified.");
    	}
    	VirtualFileOutputStream out = summaryFile.getOutputStream();
    	DataOutputStream dataOut = new DataOutputStream(out);
    	dataOut.writeInt(result.length);
    	for (int i = 0; i < result.length; ++i) {
        	dataOut.writeInt(result[i] == null ? -1 : result[i].length);
    		if (result[i] != null) {
    	    	for (int j = 0; j < result[i].length; ++j) {
    	    		result[i][j].write(dataOut);
    	    	}
    		}
    	}
    	dataOut.flush();
    	dataOut.close();
    	return summaryFile.getAbsolutePath();
    }
    /**
     * Deserializes the summary file.
     */
    public static LVColumnFile[][] readSummaryFile (VirtualFile summaryFile) throws Exception {
    	VirtualFileInputStream in = summaryFile.getInputStream();
    	DataInputStream dataIn = new DataInputStream(in);
    	LVColumnFile[][] result = new LVColumnFile[dataIn.readInt()][];
    	for (int i = 0; i < result.length; ++i) {
    		int len = dataIn.readInt();
    		assert (len >= -1);
    		if (len >= 0) {
    			result[i] = new LVColumnFile[len]; 
    			for (int j = 0; j < len; ++j) {
    				result[i][j] = LVColumnFile.read(dataIn);
    			}
    		}
    	}
    	dataIn.close();
    	return result;
    }

    /**
     * Receiving a set of completed repartitioning tasks, constructs a map of summary files
     * returned by those tasks.  
     * @param taskMap completed repartitioning tasks. the task type is probably REPARTITION,
     * but other task type is also okay as far as its output file is a summary file. 
     * @return collection of summary files
     */
    public static SortedMap<Integer, String> extractSummaryFileMap(Map<Integer, LVTask> taskMap) {
        SortedMap<Integer, String> summaryFileMap = new TreeMap<Integer, String>();
        for (LVTask task : taskMap.values()) {
            if (task.getStatus() != TaskStatus.DONE) {
                LOG.warn("this repartitioning task was not yet done. couldn't extract summary file:" + task);
                continue;
            }
            int nodeId = task.getNodeId();
            assert (!summaryFileMap.containsKey(nodeId));
            assert (task.getOutputFilePaths() != null);
            assert (task.getOutputFilePaths().length == 1);
            String summaryFilePath = task.getOutputFilePaths()[0];
            summaryFileMap.put(nodeId, summaryFilePath);
        }
        return summaryFileMap;
    }
    /**
	 * Read a set of repartitioning summary files and return the descriptors of repartitioned files.
	 */
	public static Map<Integer, LVColumnFile[][]> parseSummaryFiles (
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
	        	LVColumnFile[][] files = RepartitionSummary.readSummaryFile(summaryFile);
	        	results.put(nodeId, files);
    		} finally {
    			if (client != null) {
    				client.release();
    			}
    		}
    	}
    	return results;
	}

	/**
	 * Run tasks to delete all repartitioned files and summary file.
	 * This method assumes that the repartitioned files are under
	 * the same folder as the summary file.
	 */
	public static SortedMap<Integer, LVTask> deleteRepartitionedFiles (int jobId, LVMetadataProtocol metaRepo,
			SortedMap<Integer, String> summaryFileMap) throws IOException {
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : summaryFileMap.keySet()) {
            String summaryFilePath = summaryFileMap.get(nodeId);
            DeleteTmpFilesTaskParameters taskParam = new DeleteTmpFilesTaskParameters();
            String folder = new File(summaryFilePath).getParent();
            taskParam.setPaths(new String[]{folder});

            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.DELETE_TMP_FILES, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        return taskMap;
	}

	/**
	 * Construct one partition on memory by merging repartitioned files.
	 * @param repartitionedFiles descriptors of repartitioned files. obtained by {@link #parseSummaryFiles(DataEngineContext, SortedMap)}.
	 * @param context context of this data node.
	 * @param range the partition to merge. should be the value of {@link LVReplicaPartition#getRange()}.
	 * @param columnTypes type of output columns
	 * @param sortColumn the column to sort after merge. null to not sort.
	 * @return merged and sorted data as on-memory objects. each object is an array.
	 * @throws IOException
	 */
	public static Object[] mergeRepartitionedFilesOnMemory (
			Map<Integer, LVColumnFile[][]> repartitionedFiles, DataEngineContext context, 
			int range, ColumnType[] columnTypes, Integer sortColumn) throws IOException {
		return (Object[]) mergeRepartitionedFilesInternal(repartitionedFiles, context, range, columnTypes, sortColumn,
				true, null, null, null);
	}
	/**
	 * Construct one partition by merging repartitioned files and save it in columnar files.
     * @param outputFolder folder to output the columnar files
     * @param newFileNames name of the columnar files to output
     * @param compressions compression schemes for the columnar files to output
	 * @param repartitionedFiles descriptors of repartitioned files. obtained by {@link #parseSummaryFiles(DataEngineContext, SortedMap)}.
	 * @param context context of this data node.
	 * @param range the partition to merge. should be the value of {@link LVReplicaPartition#getRange()}.
	 * @param columnTypes type of output columns
	 * @param sortColumn the column to sort after merge. null to not sort.
	 * @return merged and sorted data as columnar files.
	 * @throws IOException
	 */
	public static ColumnFileBundle[] mergeRepartitionedFilesOnDisk (
			VirtualFile outputFolder, String[] newFileNames, CompressionType[] compressions,
			Map<Integer, LVColumnFile[][]> repartitionedFiles, DataEngineContext context, 
			int range, ColumnType[] columnTypes, Integer sortColumn
			) throws IOException {
		return (ColumnFileBundle[]) mergeRepartitionedFilesInternal(repartitionedFiles, context, range, columnTypes, sortColumn,
				false, outputFolder, newFileNames, compressions);
	}

	private static Object mergeRepartitionedFilesInternal (
			Map<Integer, LVColumnFile[][]> repartitionedFiles, DataEngineContext context, 
			int range, ColumnType[] columnTypes, Integer sortColumn, boolean onMemory,
			VirtualFile outputFolder, String[] newFileNames, CompressionType[] compressions) throws IOException {
		LOG.info("merging repartitioned files for range-" + range + ", on-memory=" + onMemory + "...");
		long startTime = System.currentTimeMillis();
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
	                LVRackNode node = context.metaRepo.getRackNode(nodeId);
	                if (node == null) {
	                    throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
	                }
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
	    	if (onMemory) {
	    		return merger.executeOnMemory();
	    	} else {
	    		return merger.executeOnDisk(outputFolder, newFileNames, compressions);
	    	}
		} finally {
			long endTime = System.currentTimeMillis();
			LOG.info("merged. elapsed time for merging repartitioned files:" + (endTime - startTime) + "ms");
			for (LVDataClient client : clients) {
				client.release();
			}
			clients.clear();
		}
	}
}
