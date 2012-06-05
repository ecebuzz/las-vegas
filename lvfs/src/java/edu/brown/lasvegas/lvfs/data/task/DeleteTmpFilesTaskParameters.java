package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.lvfs.data.DataTaskParameters;

/**
 * Parameters for  {@link DeleteTmpFilesTaskRunner}.
 */
public final class DeleteTmpFilesTaskParameters extends DataTaskParameters {
    
    /**
     * Instantiates a new delete files task parameters.
     */
    public DeleteTmpFilesTaskParameters() {
        super();
    }
    
    /**
     * Instantiates a new delete files task parameters.
     *
     * @param serializedParameters the serialized parameters
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public DeleteTmpFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    
    /**
     * Instantiates a new delete files task parameters.
     *
     * @param task the task
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public DeleteTmpFilesTaskParameters(LVTask task) throws IOException {
        super(task);
    }

    /**
     * Paths of temporary files/folders to delete.
     */
    private String[] paths;
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(paths == null ? -1 : paths.length);
        if (paths != null) {
            for (int i = 0; i < paths.length; ++i) {
                out.writeUTF(paths[i]);
            }
        }
    }

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        assert (len >= -1);
        if (len == -1) {
            paths = null;
        } else {
            paths = new String[len];
            for (int i = 0; i < len; ++i) {
                paths[i] = in.readUTF();
            }
        }
    }
    
    /**
     * Gets the paths of temporary files/folders to delete.
     *
     * @return the paths of temporary files/folders to delete
     */
    public String[] getPaths() {
        return paths;
    }
    
    /**
     * Sets the paths of temporary files/folders to delete.
     *
     * @param paths the new paths of temporary files/folders to delete
     */
    public void setPaths(String[] paths) {
        this.paths = paths;
    }
}
