package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.brown.lasvegas.JobParameters;

/**
 * Set of parameters to flush disk cache in data nodes.
 */
public final class DiskCacheFlushJobParameters extends JobParameters {
	/** true to use drop_caches (needs root permission). otherwise, simply reads large files to flood the disk cache. */
	private boolean useDropCaches;
	
    /** the files to read in each data node. key = ID of node (LVNode), value = local path. */
    private Map<Integer, String> nodeFilePathMap = new HashMap<Integer, String>();

    @Override
    public void readFields(DataInput in) throws IOException {
    	useDropCaches = in.readBoolean();
    	int len = in.readInt();
    	nodeFilePathMap.clear();
    	for (int i = 0; i < len; ++i) {
    		int nodeId = in.readInt();
    		String path = in.readUTF();
    		assert (!nodeFilePathMap.containsKey(nodeId));
    		nodeFilePathMap.put(nodeId, path);
    	}
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeBoolean(useDropCaches);
        out.writeInt(nodeFilePathMap.size());
        for (Map.Entry<Integer, String> entry : nodeFilePathMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeUTF(entry.getValue());
        }
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
	 * Gets the node file path map.
	 *
	 * @return the node file path map
	 */
	public Map<Integer, String> getNodeFilePathMap() {
		return nodeFilePathMap;
	}
	
	/**
	 * Sets the node file path map.
	 *
	 * @param nodeFilePathMap the node file path map
	 */
	public void setNodeFilePathMap(Map<Integer, String> nodeFilePathMap) {
		this.nodeFilePathMap = nodeFilePathMap;
	}
    
    
}
