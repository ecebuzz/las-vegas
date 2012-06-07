package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;
import edu.brown.lasvegas.lvfs.data.DataTaskUtil;
import edu.brown.lasvegas.lvfs.data.PartitionMergerGeneral;
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;

/**
 * Sub task of {@link JobType#RECOVER_FRACTURE_FOREIGN}.
 * In order to recover a replica from another replica that is in a different replica group,
 * a 'foreign' recovery must repartition the replica. This sub task receives the repartitioned files
 * and reconstructs the damaged partitions from them.
 * @see TaskType#RECOVER_PARTITION_FROM_REPARTITIONED_FILES
 */
public class RecoverPartitionFromRepartitionedFilesTaskRunner extends DataTaskRunner<RecoverPartitionFromRepartitionedFilesTaskParameters>{
    private static Logger LOG = Logger.getLogger(RecoverPartitionFromRepartitionedFilesTaskRunner.class);

    private LVTable table;
    private LVColumn[] columns;
    private Integer sortColumnIndex;
    private ColumnType[] columnTypes;
    private CompressionType[] compressionTypes;

    private LVReplicaGroup group;
    private LVReplicaScheme scheme;
    private LVReplica replica;
    private LVReplicaPartition[] partitions;

    private Map<Integer, LVColumnFile[][]> repartitionedFiles;//key=nodeId

    private LocalVirtualFile tmpFolder;
    private LocalVirtualFile tmpOutputFolder;
    private String[] fileTemporaryNames;

    @Override
    protected String[] runDataTask() throws Exception {
        if (parameters.getPartitionIds().length == 0) {
            LOG.warn("no inputs for this node??");
            return new String[0];
        }
        LOG.info("recovering partitions from repartitioned files...");
        prepareInputs();

        // first, we copy the repartitioned files to this node as fast as possible.
        // it's just a copy without merging/sorting.
        // the source node might die, too, so do this ASAP!
        LOG.info("copying the repartitioned files...");
        @SuppressWarnings("unchecked")
        ArrayList<ColumnFileBundle[]>[] copiedRepartitionedFiles = (ArrayList<ColumnFileBundle[]>[]) new ArrayList<?>[partitions.length];//index same as partitions
        HashMap<Integer, LVDataClient> dataClients = new HashMap<Integer, LVDataClient>(); // key= nodeID. keep this until we disconnect from source nodes
        try {
            // connect to all nodes (we can check which node has to be really accessed, but most likely it's every node)
            for (Integer nodeId : repartitionedFiles.keySet()) {
                LVRackNode node = context.metaRepo.getRackNode(nodeId);
                if (node == null) {
                    throw new IOException ("the node ID (" + nodeId + ") doesn't exist");
                }
                LVDataClient client = new LVDataClient(context.conf, node.getAddress());
                dataClients.put(nodeId, client);
            }

            for (int i = 0; i < partitions.length; ++i) {
                checkTaskCanceled();
                copiedRepartitionedFiles[i] = copyRepartitionedFile (partitions[i], dataClients, ((i + 1.0d) / (partitions.length * 2.0d)));
            }
        } finally {
            for (LVDataClient client : dataClients.values()) {
                client.release();
            }
            dataClients.clear();
        }
        LOG.info("copying done! disconnected from remote nodes");
        
        // then, sort/merge the files. at this point all accesses are local.
        LOG.info("sorting/merging the copied files...");
        for (int i = 0; i < partitions.length; ++i) {
            checkTaskCanceled();
            // merge the repartitioned files and apply sorting/compression to recover the files.
            ColumnFileBundle[] files = mergeFiles (partitions[i], copiedRepartitionedFiles[i]);
            // move files to non-temporary place
            DataTaskUtil.registerTemporaryFilesAsColumnFiles(context, partitions[i], columns, files);

            context.metaRepo.updateReplicaPartitionNoReturn(partitions[i].getPartitionId(), ReplicaPartitionStatus.OK, new IntWritable(context.nodeId));
            context.metaRepo.updateTaskNoReturn(task.getTaskId(), null, new DoubleWritable(((i + 1.0d) / (partitions.length * 2.0d)) + 0.5d), null, null);
        }

        LOG.info("all done! deleting temporary files...");
        // delete the temporary merged files
        for (ArrayList<ColumnFileBundle[]> files : copiedRepartitionedFiles) {
            if (files == null) {
                continue;
            }
            for (ColumnFileBundle[] bundles : files) {
                for (ColumnFileBundle bundle : bundles) {
                    bundle.deleteFiles();                    
                }
            }
        }
        LOG.info("deleted temporary files");


        return new String[0];
    }
    private ArrayList<ColumnFileBundle[]> copyRepartitionedFile (LVReplicaPartition partition, HashMap<Integer, LVDataClient> dataClients, double completedProgress) throws IOException {
        LOG.info("copying files for " + partition.getRange() + "th partition");
        ArrayList<ColumnFileBundle[]> copiedFiles = new ArrayList<ColumnFileBundle[]>();
        for (Integer nodeId : repartitionedFiles.keySet()) {
            LVColumnFile[][] files = repartitionedFiles.get(nodeId);
            if (files == null) {
                continue;
            }
            if (files.length <= partition.getRange()) {
                // this is weird
                LOG.warn("wtf. the repartitioned file array seems too short to contain the " + partition.getRange() + "th partition. ignored");
                continue;
            }
            LVColumnFile[] columnFiles = files[partition.getRange()];
            if (columnFiles == null) {
                continue; // this is fine. it can happen
            }
            assert (columnFiles.length == columns.length);
            ColumnFileBundle[] bundles = new ColumnFileBundle[columnFiles.length];
            if (nodeId == context.nodeId) {
                // it's already in this node!
                for (int i = 0; i < columnFiles.length; ++i) {
                    bundles[i] = new ColumnFileBundle(columnFiles[i]);
                }
            } else {
                // it's remote. so we need to copy
                LVDataClient client = dataClients.get(nodeId);
                assert (client != null);
                for (int i = 0; i < columnFiles.length; ++i) {
                    ColumnFileBundle remoteFile = new ColumnFileBundle(columnFiles[i], client.getChannel());
                    bundles[i] = remoteFile.copyFiles(tmpOutputFolder);
                }
                
            }
            copiedFiles.add(bundles);
        }
        return copiedFiles;
    }


    private ColumnFileBundle[] mergeFiles (LVReplicaPartition partition, ArrayList<ColumnFileBundle[]> copiedFiles) throws IOException {
        LOG.info("merging/sorting the merged files for " + partition.getRange() + "th partition");
        PartitionMergerGeneral merger = new PartitionMergerGeneral((ColumnFileBundle[][]) copiedFiles.toArray(), columnTypes, sortColumnIndex);
        ColumnFileBundle[] merged = merger.executeOnDisk(tmpOutputFolder, fileTemporaryNames, compressionTypes);
        LOG.info("merged and sorted!");
        return merged;
    }

    private void prepareInputs () throws Exception {
        replica = context.metaRepo.getReplica(parameters.getReplicaId());
        if (replica == null) {
            throw new IOException("this replica ID doesn't exist:" + parameters.getReplicaId());
        }
        scheme = context.metaRepo.getReplicaScheme(replica.getSchemeId());
        if (scheme == null) {
            throw new IOException("this replica scheme ID doesn't exist:" + replica.getSchemeId());
        }

        group = context.metaRepo.getReplicaGroup(scheme.getGroupId());
        assert (group != null);
        
        table = context.metaRepo.getTable(group.getTableId());
        if (table == null) {
            throw new IOException ("this table ID doesn't exist:" + group.getTableId());
        }

        columns = context.metaRepo.getAllColumnsExceptEpochColumn(group.getTableId());
        columnTypes = new ColumnType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            columnTypes[i] = columns[i].getType();
        }
        
        compressionTypes = new CompressionType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            compressionTypes[i] = scheme.getColumnCompressionScheme(columns[i].getColumnId());
        }

        partitions = new LVReplicaPartition[parameters.getPartitionIds().length];
        for (int i = 0; i < parameters.getPartitionIds().length; ++i) {
            int partitionId = parameters.getPartitionIds()[i];
            LVReplicaPartition partition = context.metaRepo.getReplicaPartition(partitionId);
            if (partition == null) {
                throw new IOException("this replica partition ID doesn't exist:" + partitionId);
            }
            if (partition.getReplicaId() != replica.getReplicaId()) {
                throw new IOException("the replica partition doesn't belong to:" + replica);
            }
            assert (partition.getReplicaGroupId() == scheme.getGroupId());
            partitions[i] = partition;
        }

        tmpFolder = new LocalVirtualFile (context.localLvfsTmpDir);
        tmpOutputFolder = tmpFolder.getChildFile("recover_tmp_" + Math.abs(new Random(System.nanoTime()).nextInt()));
        tmpOutputFolder.mkdirs();
        if (!tmpOutputFolder.exists()) {
            throw new IOException ("failed to create a temporary output folder: " + tmpOutputFolder.getAbsolutePath());
        }

        fileTemporaryNames =  new String[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            fileTemporaryNames[i] = "recovered_" + i;
        }

        repartitionedFiles = RepartitionSummary.parseSummaryFiles(context, parameters.getRepartitionSummaryFileMap());

        sortColumnIndex = null;
        if (scheme.getSortColumnId() != null) {
            for (int i = 0; i < columns.length; ++i) {
                if (columns[i].getColumnId() == scheme.getSortColumnId().intValue()) {
                    sortColumnIndex = i;
                    break;
                }
            }
            assert (sortColumnIndex != null);
        }
    }
}
