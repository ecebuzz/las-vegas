package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TaskType;

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
    private LVFracture fracture;
    private LVReplica replica;
    private LVReplicaScheme scheme;
    private Map<Integer, PartitionInput> partitionInputs;

    /** temporary output files (not including the final output files registered as LVColumnFile). */
    private List<String> outputFiles;
    
    @Override
    protected String[] runDataTask() throws Exception {
        LOG.info("loading partitioned text files...");
        prepareInputs ();
        checkTaskCanceled();
        for (PartitionInput partitionInput : partitionInputs.values()) {
            loadPartition (partitionInput);
        }
        LOG.info("done!");
        return outputFiles.toArray(new String[0]);
    }
    /** load one partition. */
    private void loadPartition(PartitionInput partitionInput) throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info ("importing replica partition:" + partitionInput.partition);
        }
        
        // first, sequentially copy all input files without sorting.
        // no compression except dictionary encoding at this point.
        for (int i = 0; i < columns.length; ++i) {
            // columns[i].getType()
        }
        // scheme.getColumnCompressionScheme(columnId);
        
        for (TemporaryFilePath path : partitionInput.inputFilePaths) {
            if (path.nodeId == context.nodeId) {
                // it's local! simply use java.io.File
            } else {
                // it's remote. Connect to the node
            }
        }
    }
    
    private static class PartitionInput {
        PartitionInput (LVReplicaPartition partition) {
            this.partition = partition;
        }
        LVReplicaPartition partition;
        List<TemporaryFilePath> inputFilePaths = new ArrayList<TemporaryFilePath>();
    }
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
    }
}
