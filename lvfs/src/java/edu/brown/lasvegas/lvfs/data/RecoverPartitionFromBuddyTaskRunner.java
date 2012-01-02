package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

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
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;

/**
 * Sub task of {@link JobType#IMPORT_FRACTURE} or recovery jobs such as {@link JobType#RECOVER_FRACTURE_FROM_BUDDY}.
 * Assuming a buddy (another replica scheme in the same replica group) has all
 * column files of a partition, this task reads, sorts, and compresses them to
 * its own column files. This task is supposed to be efficient because the communication will
 * be between nodes in the same rack.
 * @see TaskType#RECOVER_PARTITION_FROM_BUDDY
 */
public final class RecoverPartitionFromBuddyTaskRunner extends DataTaskRunner<RecoverPartitionFromBuddyTaskParameters>{
    private static Logger LOG = Logger.getLogger(RecoverPartitionFromBuddyTaskRunner.class);
    
    private LVTable table;
    private LVColumn[] columns;
    private ColumnType[] columnTypes;

    private CompressionType[] compressionTypes;
    private CompressionType[] buddyCompressionTypes;
    private LVReplicaGroup group;
    private LVReplicaScheme scheme;
    private LVReplicaScheme buddyScheme;
    private LVReplica replica;
    private LVReplica buddyReplica;
    private LVReplicaPartition[] partitions;
    private LVReplicaPartition[] buddyPartitions;

    private VirtualFile tmpFolder;
    private VirtualFile tmpOutputFolder;
    private String[] fileTemporaryNames;

    @Override
    protected String[] runDataTask() throws Exception {
        if (parameters.getPartitionIds().length == 0) {
            LOG.warn("no inputs for this node??");
            return new String[0];
        }
        LOG.info("recovering partitions from a buddy replica...");
        prepareInputs();
        for (int i = 0; i < partitions.length; ++i) {
            checkTaskCanceled();
            // apply sorting/compression to recover the files
            ColumnFileBundle[] files = recoverPatition (partitions[i], buddyPartitions[i]);
            // move files to non-temporary place
            moveFiles (files);
        }
        //TODO implement
        checkTaskCanceled();
        LOG.info("done!");
        return new String[0];
    }
    
    private ColumnFileBundle[] recoverPatition (LVReplicaPartition partition, LVReplicaPartition buddyPartition) throws IOException {
        LOG.info("recovering partition " + partition + " from the buddy:" + buddyPartition);
        LVColumnFile[] buddyColumnFiles = context.metaRepo.getAllColumnFilesByReplicaPartitionId(buddyPartition.getPartitionId());
        ColumnFileBundle[] buddies = new ColumnFileBundle[buddyColumnFiles.length];
        int buddyNodeId = buddyPartition.getNodeId();
        HashMap<Integer, LVDataClient> dataClients = new HashMap<Integer, LVDataClient>(); // key= nodeID. keep this until we disconnect from data nodes
        try {
            if (buddyNodeId == context.nodeId) {
                // it's in same node!
                for (int i = 0; i < buddyColumnFiles.length; ++i) {
                    buddies[i] = new ColumnFileBundle(buddyColumnFiles[i]);
                }
            } else {
                // it's remote. Connect to the node
                LVDataClient client = dataClients.get(buddyNodeId);
                if (client == null) {
                    LVRackNode node = context.metaRepo.getRackNode(buddyNodeId);
                    if (node == null) {
                        throw new IOException ("the node ID (" + buddyNodeId + ") doesn't exist");
                    }
                    client = new LVDataClient(context.conf, node.getAddress());
                    dataClients.put(buddyNodeId, client);
                }
                for (int i = 0; i < buddyColumnFiles.length; ++i) {
                    buddies[i] = new ColumnFileBundle(buddyColumnFiles[i], client.getChannel());
                }
            }
            PartitionRewriter rewriter = new PartitionRewriter(tmpOutputFolder, buddies, fileTemporaryNames, compressionTypes, scheme.getSortColumnId());
            return rewriter.execute();
        } finally {
            for (LVDataClient client : dataClients.values()) {
                client.release();
            }
            dataClients.clear();
        }
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
        buddyReplica = context.metaRepo.getReplica(parameters.getBuddyReplicaId());
        if (buddyReplica == null) {
            throw new IOException("this replica ID doesn't exist:" + parameters.getBuddyReplicaId());
        }
        buddyScheme = context.metaRepo.getReplicaScheme(buddyReplica.getSchemeId());
        if (buddyScheme == null) {
            throw new IOException("this replica scheme ID doesn't exist:" + buddyReplica.getSchemeId());
        }
        assert (scheme.getSchemeId() != buddyScheme.getSchemeId());
        assert (scheme.getGroupId() == buddyScheme.getGroupId());

        group = context.metaRepo.getReplicaGroup(scheme.getGroupId());
        assert (group != null);
        
        table = context.metaRepo.getTable(group.getTableId());
        if (table == null) {
            throw new IOException ("this table ID doesn't exist:" + group.getTableId());
        }

        columns = context.metaRepo.getAllColumns(group.getTableId());
        columnTypes = new ColumnType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            columnTypes[i] = columns[i].getType();
        }
        
        compressionTypes = new CompressionType[columns.length];
        buddyCompressionTypes = new CompressionType[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            compressionTypes[i] = scheme.getColumnCompressionScheme(columns[i].getColumnId());
            buddyCompressionTypes[i] = buddyScheme.getColumnCompressionScheme(columns[i].getColumnId());
        }

        partitions = new LVReplicaPartition[parameters.getPartitionIds().length];
        buddyPartitions = new LVReplicaPartition[partitions.length];
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
            
            LVReplicaPartition buddyPartition = context.metaRepo.getReplicaPartitionByReplicaAndRange(buddyReplica.getReplicaId(), partition.getRange());
            if (buddyPartition == null || buddyPartition.getStatus() != ReplicaPartitionStatus.OK) {
                throw new IOException ("the buddy partition doesn't exist or is not ready: " + buddyPartition);
            }
            if (buddyPartition.getNodeId() == null) {
                throw new IOException ("the buddy partition isn't assigned to data node: " + buddyPartition);
            }
            buddyPartitions[i] = buddyPartition;
        }

        tmpFolder = new LocalVirtualFile (context.localLvfsTmpDir);
        tmpOutputFolder = tmpFolder.getChildFile("recover_tmp_" + new Random(System.nanoTime()).nextInt());
        tmpOutputFolder.mkdirs();
        if (!tmpOutputFolder.exists()) {
            throw new IOException ("failed to create a temporary output folder: " + tmpOutputFolder.getAbsolutePath());
        }

        fileTemporaryNames =  new String[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            fileTemporaryNames[i] = "recovered_" + i;
        }
    }
    private void moveFiles (ColumnFileBundle[] files) throws IOException {
        // TODO implement : share the code in Load task
    }
}
