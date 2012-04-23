package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.lvfs.data.DataTaskParameters;

/**
 * Parameters for {@link DiskCacheFlushTaskRunner}.
 */
public class DiskCacheFlushTaskParameters extends DataTaskParameters {
	/** true to use drop_caches (needs root permission). otherwise, simply reads large files to flood the disk cache. */
	private boolean useDropCaches;
	
    /** the path of the file to read in this data node. */
    private String path;

    @Override
    public void readFields(DataInput in) throws IOException {
    	useDropCaches = in.readBoolean();
    	path = in.readUTF();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeBoolean(useDropCaches);
    	out.writeUTF(path);
    }
	
	/**
	 * Checks if is use drop caches.
	 *
	 * @return true, if is use drop caches
	 */
	public boolean isUseDropCaches() {
		return useDropCaches;
	}
	
	/**
	 * Sets the use drop caches.
	 *
	 * @param useDropCaches the new use drop caches
	 */
	public void setUseDropCaches(boolean useDropCaches) {
		this.useDropCaches = useDropCaches;
	}
	
	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public String getPath() {
		return path;
	}
	
	/**
	 * Sets the path.
	 *
	 * @param path the new path
	 */
	public void setPath(String path) {
		this.path = path;
	}
}
