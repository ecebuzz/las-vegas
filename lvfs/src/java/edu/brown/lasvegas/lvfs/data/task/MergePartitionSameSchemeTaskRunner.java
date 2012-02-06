package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;
import edu.brown.lasvegas.lvfs.data.DataTaskUtil;
import edu.brown.lasvegas.lvfs.data.PartitionMergerForSameScheme;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;

/**
 * Sub task of {@link JobType#MERGE_FRACTURE}.
 * Given existing ReplicaPartition in the same replica scheme,
 * merge them into one file.
 * This task has low CPU-overhead because it assumes base partitions in the same scheme (sorting).
 * However, it might cause additional network I/O because some other replica scheme might have
 * corresponding partitions in the same node or at least in the same rack although
 * it needs re-sorting to use.
 * Another version of this task (MergePartitionDifferentScheme?) might be added later to see
 * the tradeoff.
 * @see TaskType#MERGE_PARTITION_SAME_SCHEME
 */
public final class MergePartitionSameSchemeTaskRunner extends DataTaskRunner<MergePartitionSameSchemeTaskParameters> {
    private static Logger LOG = Logger.getLogger(MergePartitionSameSchemeTaskRunner.class);
    
    private LVTable table;
    private LVColumn[] columns;
    private ColumnType[] columnTypes;
    private CompressionType[] compressions;

    private LVReplicaScheme scheme;

    private LVFracture newFracture;
    private LVReplica newReplica;
    private LVReplicaPartition newPartition;

    private LVReplicaPartition[] basePartitions;
    
    private VirtualFile tmpFolder;
    private VirtualFile tmpOutputFolder;
    private String[] newFileTemporaryNames;
    
    @Override
    protected String[] runDataTask() throws Exception {
        LOG.info("merging based on " + parameters.getBasePartitionIds().length + " partitions..");
        prepareInputs ();
        checkTaskCanceled();

        ColumnFileBundle[] newFiles;
        HashMap<Integer, LVDataClient> dataClients = new HashMap<Integer, LVDataClient>(); // key= nodeID. keep this until we disconnect from data nodes
        try {
            // prepare baseFiles. it might be remote
            ColumnFileBundle[][] baseFiles = new ColumnFileBundle[basePartitions.length][columns.length];
            for (int i = 0; i < basePartitions.length; ++i) {
                LVReplicaPartition base = basePartitions[i];
                LVColumnFile[] files = context.metaRepo.getAllColumnFilesByReplicaPartitionId(base.getPartitionId());
                assert (columns.length == files.length);
                if (base.getNodeId().intValue() == context.nodeId) {
                    // it's in same node!
                    for (int j = 0; j < files.length; ++j) {
                        baseFiles[i][j] = new ColumnFileBundle(files[j]);
                    }
                } else {
                    // it's remote. Connect to the node
                    LVDataClient client = dataClients.get(base.getNodeId());
                    if (client == null) {
                        LVRackNode node = context.metaRepo.getRackNode(base.getNodeId());
                        if (node == null) {
                            throw new IOException ("the node ID (" + base.getNodeId() + ") doesn't exist");
                        }
                        client = new LVDataClient(context.conf, node.getAddress());
                        dataClients.put(base.getNodeId(), client);
                    }
                    for (int j = 0; j < files.length; ++j) {
                        baseFiles[i][j] = new ColumnFileBundle(files[j], client.getChannel());
                    }
                }
            }

            PartitionMergerForSameScheme merger = new PartitionMergerForSameScheme(tmpOutputFolder, baseFiles, newFileTemporaryNames, columnTypes, compressions, scheme.getSortColumnId());
            newFiles = merger.execute();
        } finally {
            for (LVDataClient client : dataClients.values()) {
                client.release();
            }
            dataClients.clear();
        }

        // move files to non-temporary place
        DataTaskUtil.registerTemporaryFilesAsColumnFiles(context, newPartition, columns, newFiles);
        
        LOG.info("done!");
        return new String[0];
    }

    private void prepareInputs () throws Exception {
        newPartition = context.metaRepo.getReplicaPartition(parameters.getNewPartitionId());
        if (newPartition == null) {
            throw new IOException ("this partition ID doesn't exist:" + parameters.getNewPartitionId());
        }
        newReplica = context.metaRepo.getReplica(newPartition.getReplicaId());
        assert(newReplica != null);
        newFracture = context.metaRepo.getFracture(newReplica.getFractureId());
        assert(newFracture != null);
        scheme = context.metaRepo.getReplicaScheme(newReplica.getSchemeId());
        assert(scheme != null);
        table = context.metaRepo.getTable(newFracture.getTableId());
        assert(table != null);

        columns = context.metaRepo.getAllColumnsExceptEpochColumn(table.getTableId());
        columnTypes = new ColumnType[columns.length];
        compressions = new CompressionType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            columnTypes[i] = columns[i].getType();
            compressions[i] = scheme.getColumnCompressionScheme(columns[i].getColumnId());
        }
        
        basePartitions = new LVReplicaPartition[parameters.getBasePartitionIds().length];
        for (int i = 0; i < parameters.getBasePartitionIds().length; ++i) {
            int id = parameters.getBasePartitionIds()[i];
            basePartitions[i] = context.metaRepo.getReplicaPartition(id);
            if (basePartitions[i] == null) {
                throw new IOException ("this partition ID doesn't exist:" + id);
            }
            if (basePartitions[i].getRange() != newPartition.getRange()) {
                throw new IOException ("not a corresponding partition :" + basePartitions[i]);
            }
        }

        tmpFolder = new LocalVirtualFile (context.localLvfsTmpDir);
        tmpOutputFolder = tmpFolder.getChildFile("merge_tmp_" + Math.abs(new Random(System.nanoTime()).nextInt()));
        tmpOutputFolder.mkdirs();
        if (!tmpOutputFolder.exists()) {
            throw new IOException ("failed to create a temporary output folder: " + tmpOutputFolder.getAbsolutePath());
        }

        newFileTemporaryNames =  new String[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            newFileTemporaryNames[i] = "tmp_" + i;
        }
    }
}
