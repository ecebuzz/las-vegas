package edu.brown.lasvegas.lvfs.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;

/**
 * Parameters for {@link LoadPartitionedTextFilesTaskRunner}.
 */
public final class LoadPartitionedTextFilesTaskParameters extends TextFileImportTaskParameters {
    /** ID of {@link LVReplica} to import at this node. */
    private int replicaId;
    /** IDs of {@link LVReplicaPartition} to import at this node. All of these should belong to the specified replica. */
    private int[] replicaPartitionIds;
    
    /**
     * path of temporary partitioned files from all nodes.
     * The file names will tell their replica group ID, partitions, and Node ID.
     * (see {@link PartitionedTextFileWriters} for the temporary file naming rules).
     */
    private String[] temporaryPartitionedFiles;
    
    @Override
    protected void writeDerived(DataOutput out) throws IOException {
        out.writeInt(replicaId);
        if (replicaPartitionIds == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(replicaPartitionIds.length);
            for (int id : replicaPartitionIds) {
                out.writeInt(id);
            }
        }
        if (temporaryPartitionedFiles == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(temporaryPartitionedFiles.length);
            for (String path : temporaryPartitionedFiles) {
                out.writeUTF(path);
            }
        }
    }

    @Override
    protected void readFieldsDerived(DataInput in) throws IOException {
        replicaId = in.readInt();
        {
            int len = in.readInt();
            if (len < 0) {
                replicaPartitionIds = null;
            } else {
                replicaPartitionIds = new int[len];
                for (int i = 0; i < len; ++i) {
                    replicaPartitionIds[i] = in.readInt();
                }
            }
        }        
        {
            int len = in.readInt();
            if (len < 0) {
                temporaryPartitionedFiles = null;
            } else {
                temporaryPartitionedFiles = new String[len];
                for (int i = 0; i < len; ++i) {
                    temporaryPartitionedFiles[i] = in.readUTF();
                }
            }
        }
    }
    
    public LoadPartitionedTextFilesTaskParameters() {
        super();
    }
    public LoadPartitionedTextFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public LoadPartitionedTextFilesTaskParameters(LVTask task) throws IOException {
        super(task);
    }
    
    // auto-generated getters/setters (comments by JAutodoc)    
    /**
     * Gets the path of temporary partitioned files at each node.
     *
     * @return the path of temporary partitioned files at each node
     */
    public String[] getTemporaryPartitionedFiles() {
        return temporaryPartitionedFiles;
    }
    
    /**
     * Sets the path of temporary partitioned files at each node.
     *
     * @param temporaryPartitionedFiles the new path of temporary partitioned files at each node
     */
    public void setTemporaryPartitionedFiles(String[] temporaryPartitionedFiles) {
        this.temporaryPartitionedFiles = temporaryPartitionedFiles;
    }

    /**
     * Gets the iDs of {@link LVReplicaPartition} to import at this node.
     *
     * @return the iDs of {@link LVReplicaPartition} to import at this node
     */
    public int[] getReplicaPartitionIds() {
        return replicaPartitionIds;
    }

    /**
     * Sets the iDs of {@link LVReplicaPartition} to import at this node.
     *
     * @param replicaPartitionIds the new iDs of {@link LVReplicaPartition} to import at this node
     */
    public void setReplicaPartitionIds(int[] replicaPartitionIds) {
        this.replicaPartitionIds = replicaPartitionIds;
    }

    /**
     * Gets the iD of {@link LVReplica} to import at this node.
     *
     * @return the iD of {@link LVReplica} to import at this node
     */
    public int getReplicaId() {
        return replicaId;
    }

    /**
     * Sets the iD of {@link LVReplica} to import at this node.
     *
     * @param replicaId the new iD of {@link LVReplica} to import at this node
     */
    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }

    
}
