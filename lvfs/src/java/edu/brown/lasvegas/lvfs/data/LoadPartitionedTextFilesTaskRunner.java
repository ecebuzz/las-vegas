package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.tuple.BufferedTupleWriter;
import edu.brown.lasvegas.tuple.TextFileTupleReader;

/**
 * Sub task of {@link JobType#IMPORT_FRACTURE}.
 * Given partitioned text files output by {@link #PARTITION_TEXT_FILES} task,
 * this task collects those text files from local and remote nodes and
 * construct LVFS files in the local drive.
 * This task is for the first replica scheme in each replica group.
 * For other replica schemes in the group (buddy), use
 * #RECOVER_PARTITION_FROM_BUDDY for much better performance.
 * @see TaskType#LOAD_PARTITIONED_TEXT_FILES
 */
public final class LoadPartitionedTextFilesTaskRunner extends DataTaskRunner<LoadPartitionedTextFilesTaskParameters> {
    private static Logger LOG = Logger.getLogger(LoadPartitionedTextFilesTaskRunner.class);
    
    private LVTable table;
    private LVColumn[] columns;
    private ColumnType[] columnTypes;
    private LVFracture fracture;
    private LVReplica replica;
    private LVReplicaScheme scheme;
    private Map<Integer, PartitionInput> partitionInputs;
    
    private CompressionType[] temporaryCompressionTypes;

    private Charset charset;
    private DateFormat dateFormat;
    private DateFormat timeFormat;
    private DateFormat timestampFormat;

    private VirtualFile tmpFolder;
    private VirtualFile tmpOutputFolder;
    private String[] unsortedFileTemporaryNames;
    private String[] sortedFileTemporaryNames;

    /** temporary output files (not including the final output files registered as LVColumnFile). */
    private List<String> outputFiles;
    
    @Override
    protected String[] runDataTask() throws Exception {
        LOG.info("loading partitioned text files...");
        prepareInputs ();
        checkTaskCanceled();
        for (PartitionInput partitionInput : partitionInputs.values()) {
            loadPartition (partitionInput);
            checkTaskCanceled();
        }
        LOG.info("done!");
        return outputFiles.toArray(new String[0]);
    }
    /** load one partition. */
    private void loadPartition(PartitionInput partitionInput) throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info ("importing replica partition:" + partitionInput.partition);
        }

        // first, create column files without sorting
        ColumnFileBundle[] unsortedFiles = writeUnsortedFiles (partitionInput);
        
        if (scheme.getSortColumnId() == null) {
            // if no sorting is needed, it's done.
            // TODO move files
        } else {
            // otherwise, we need to rewrite the files to sort and compress them
        }
        
    }
    /**
     * sequentially copy all input files into unsorted column files.
     * Returns the written column files.
     */
    private ColumnFileBundle[] writeUnsortedFiles (PartitionInput partitionInput) throws Exception {
        HashMap<Integer, LVDataClient> dataClients = new HashMap<Integer, LVDataClient>(); // key= nodeID. keep this until we disconnect from data nodes
        // convert input files for this partition to VirtualFile
        ArrayList<VirtualFile> virtualFiles = new ArrayList<VirtualFile>();
        for (TemporaryFilePath path : partitionInput.inputFilePaths) {
            VirtualFile file;
            if (path.nodeId == context.nodeId) {
                // it's local! simply use java.io.File
                file = new LocalVirtualFile(path.getFilePath());
            } else {
                // it's remote. Connect to the node
                LVDataClient client = dataClients.get(path.nodeId);
                if (client == null) {
                    LVRackNode node = context.metaRepo.getRackNode(path.nodeId);
                    if (node == null) {
                        throw new IOException ("the node ID (" + path.nodeId + ") contained in the temporary file path " + path.getFilePath() + " isn't found");
                    }
                    client = new LVDataClient(context.conf, node.getName()); // TODO this should be node.getAddress()
                    dataClients.put(path.nodeId, client);
                }
                file = new DataNodeFile(client.getChannel(), path.getFilePath());
                if (!file.exists()) {
                    throw new IOException ("the temporary file " + file.getAbsolutePath() + " doesn't exist on node-" + path.nodeId);
                }
            }
            virtualFiles.add(file);
        }
        checkTaskCanceled();

        VirtualFile[] inputFiles = virtualFiles.toArray(new VirtualFile[0]);
        TextFileTupleReader reader = new TextFileTupleReader(inputFiles, parameters.getTemporaryCompression(),
            columnTypes, parameters.getDelimiter(), 1 << 20, charset, dateFormat, timeFormat, timestampFormat);
        CheckCanceledCallback callback = new CheckCanceledCallback();
        // if we don't have to sort after this. the files will be the final output. So, let's calculate checksum at this point
        boolean calculateChecksum = scheme.getSortColumnId() == null;
        BufferedTupleWriter writer = new BufferedTupleWriter(reader, 1 << 13, tmpOutputFolder, temporaryCompressionTypes, unsortedFileTemporaryNames, calculateChecksum, callback);
        writer.appendAllTuples();
        reader.close();
        if (callback.taskCanceled) {
            // exit ASAP, but release the resources before that
            writer.close();
            for (LVDataClient client : dataClients.values()) {
                client.release();
            }
            throw new TaskCanceledException ();
        }
        ColumnFileBundle[] writtenFiles = writer.finish();
        writer.close();

        // now we can disconnect from the data node
        for (LVDataClient client : dataClients.values()) {
            client.release();
        }

        checkTaskCanceled();
        LOG.info("wrote " + writer.getTupleCount() + " tuples in total");
        context.metaRepo.updateTaskNoReturn(task.getTaskId(), null, new DoubleWritable(0.5d), null, null); // well, largely 50%.
        return writtenFiles;
    }
    
    
    /** callback object to periodically check if this task is canceled. */
    private class CheckCanceledCallback implements BufferedTupleWriter.BatchCallback {
        private boolean taskCanceled = false;
        private int previousCheckedTuple = 0;
        private static final int CHECK_INTERVAL_TUPLES = 100000;
        @Override
        public boolean onBatchWritten(int totalTuplesWritten) throws IOException {
            if (totalTuplesWritten - previousCheckedTuple >= CHECK_INTERVAL_TUPLES) {
                boolean canceled = isTaskCanceled();
                if (canceled) {
                    // then, let's exit ASAP
                    taskCanceled = true;
                    return false;
                }
                previousCheckedTuple = totalTuplesWritten;
            }
            return true;// otherwise go on
        }
    }
    
    private static class PartitionInput {
        PartitionInput (LVReplicaPartition partition) {
            this.partition = partition;
        }
        LVReplicaPartition partition;
        List<TemporaryFilePath> inputFilePaths = new ArrayList<TemporaryFilePath>();
    }
    /** divides the inputs to each partition. */
    private void prepareInputs () throws Exception {
        fracture = context.metaRepo.getFracture(parameters.getFractureId());
        if (fracture == null) {
            throw new IOException ("this fracture ID doesn't exist:" + parameters.getFractureId());
        }

        table = context.metaRepo.getTable(fracture.getTableId());
        if (table == null) {
            throw new IOException ("this table ID doesn't exist:" + fracture.getTableId());
        }

        columns = context.metaRepo.getAllColumns(fracture.getTableId());
        columnTypes = new ColumnType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            columnTypes[i] = columns[i].getType();
        }

        replica = context.metaRepo.getReplica(parameters.getReplicaId());
        if (replica == null) {
            throw new IOException ("this Replica ID doesn't exist:" + parameters.getReplicaId());
        }

        scheme = context.metaRepo.getReplicaScheme(replica.getSchemeId());
        if (scheme == null) {
            throw new IOException ("this Replica Scheme ID doesn't exist:" + replica.getSchemeId());
        }

        partitionInputs = new TreeMap<Integer, PartitionInput>();
        for (int id : parameters.getReplicaPartitionIds()) {
            LVReplicaPartition partition = context.metaRepo.getReplicaPartition(id);
            if (partition == null) {
                throw new IOException ("this sub-partition ID doesn't exist:" + id);
            }
            if (partition.getReplicaId() != parameters.getReplicaId()) {
                throw new IOException ("replica ID didn't match:" + parameters.getReplicaId() + ", " + partition);
            }
            if (partition.getNodeId() == null || partition.getNodeId().intValue() != context.nodeId) {
                throw new IOException ("this sub-partition isn't assigned to this node:" + partition);
            }
            partitionInputs.put(partition.getRange(), new PartitionInput(partition));
        }
        for (String path : parameters.getTemporaryPartitionedFiles()) {
            TemporaryFilePath pathParsed = new TemporaryFilePath(path);
            if (pathParsed.fractureId != parameters.getFractureId()) {
                throw new IOException ("fracture ID doesn't match:" + path);
            }
            if (pathParsed.replicaGroupId != scheme.getGroupId()) {
                throw new IOException ("replica Group ID doesn't match:" + path);
            }
            PartitionInput input = partitionInputs.get(pathParsed.partition);
            if (input == null) {
                throw new IOException ("partition mismatch:" + path);
            }
            if (pathParsed.compression != parameters.getTemporaryCompression()) {
                throw new IOException ("compression type mismatch:" + path);
            }
            input.inputFilePaths.add(pathParsed);
        }
        outputFiles = new ArrayList<String>();

        charset = Charset.forName(parameters.getEncoding());
        dateFormat = new SimpleDateFormat(parameters.getDateFormat());
        timeFormat = new SimpleDateFormat(parameters.getTimeFormat());
        timestampFormat = new SimpleDateFormat(parameters.getTimestampFormat());

        tmpFolder = new LocalVirtualFile (context.localLvfsTmpDir);
        tmpOutputFolder = tmpFolder.getChildFile("load_tmp_" + new Random(System.nanoTime()).nextInt());
        tmpOutputFolder.mkdirs();
        if (!tmpOutputFolder.exists()) {
            throw new IOException ("failed to create a temporary output folder: " + tmpOutputFolder.getAbsolutePath());
        }

        // no compression except dictionary encoding at this point
        // because we will soon re-read/write the files to sort them. (well, unless the Replica Scheme has no sorting) 
        temporaryCompressionTypes = new CompressionType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            temporaryCompressionTypes[i] = scheme.getColumnCompressionScheme(columns[i].getColumnId());
            if (scheme.getSortColumnId() != null && temporaryCompressionTypes[i] != CompressionType.DICTIONARY) {
                temporaryCompressionTypes[i] = CompressionType.NONE;
            }
        }
        unsortedFileTemporaryNames = new String[columns.length];
        sortedFileTemporaryNames =  new String[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            unsortedFileTemporaryNames[i] = "unsorted_" + i;
            sortedFileTemporaryNames[i] = "sorted_" + i;
        }
    }
}
