package edu.brown.lasvegas.server;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

public class ConfFileUtil {
    /**
     * adds the given path to default configuration path and 
     * checks if the given path (relative to classpath) points to a valid configuration file for LVFS.
     */
    public static final void addConfFilePath (String path) throws IOException {
        try {
            InputStream test = ConfFileUtil.class.getResourceAsStream(path);
            test.read();
            test.close();
        } catch (Exception ex) {
            throw new IOException (path + " cannot be read as a resource. Is it in the classpath?");
        }
        // Configuration.addDefaultResource(path);
        
        // at least it should have the central node's address
        Configuration conf = new Configuration();
        conf.addResource(ConfFileUtil.class.getResource(path));
        String metaRepoAddresss = conf.get(LVCentralNode.METAREPO_ADDRESS_KEY);
        if (metaRepoAddresss == null || metaRepoAddresss.length() == 0) {
            throw new IOException ("It seems the file " + path + " doesn't even define " + LVCentralNode.METAREPO_ADDRESS_KEY);
        }
    }
    
    private ConfFileUtil() {}
}
