package edu.brown.lasvegas.server;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.client.LVMetadataClient;

/**
 * A short program to gracefully request the central node to stop.
 * Ctrl-C could do the job, but this one is more flexible and safe.
 */
public class StopCentralNode {
    public static void main (String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: java " + StopCentralNode.class.getName() + " <conf xml path in classpath>");
            System.err.println("ex: java " + StopCentralNode.class.getName() + " lvfs_conf.xml");
            return;
        }
        ConfFileUtil.addConfFilePath(args[0]);
        
        Configuration conf = new Configuration();
        LVMetadataClient client = new LVMetadataClient(conf);
        client.getChannel().shutdown();
        Thread.sleep(200L);
        client.release();
        
        System.out.println ("requested to shutdown");
    }
}
