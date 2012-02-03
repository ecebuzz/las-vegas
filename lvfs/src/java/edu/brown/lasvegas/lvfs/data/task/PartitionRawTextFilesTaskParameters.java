package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.TextFileImportTaskParameters;

/**
 * Parameters for {@link PartitionRawTextFilesTaskRunner}.
 */
public final class PartitionRawTextFilesTaskParameters extends TextFileImportTaskParameters {
    /** the local path of files to import in this data node. */
    private String[] filePaths;

    @Override
    protected void writeDerived(DataOutput out) throws IOException {
        if (filePaths == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(filePaths.length);
            for (String path : filePaths) {
                out.writeUTF(path);
            }
        }
    }

    @Override
    protected void readFieldsDerived(DataInput in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            filePaths = null;
        } else {
            filePaths = new String[len];
            for (int i = 0; i < len; ++i) {
                filePaths[i] = in.readUTF();
            }
        }
    }
    
    public PartitionRawTextFilesTaskParameters() {
        super();
    }
    public PartitionRawTextFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public PartitionRawTextFilesTaskParameters(LVTask task) throws IOException {
        super(task);
    }
    
    // auto-generated getters/setters (comments by JAutodoc)    
    /**
     * Gets the local path of files to import in this data node.
     *
     * @return the local path of files to import in this data node
     */
    public String[] getFilePaths() {
        return filePaths;
    }
    
    /**
     * Sets the local path of files to import in this data node.
     *
     * @param filePaths the new local path of files to import in this data node
     */
    public void setFilePaths(String[] filePaths) {
        this.filePaths = filePaths;
    }
}