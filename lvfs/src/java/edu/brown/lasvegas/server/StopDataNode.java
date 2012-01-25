package edu.brown.lasvegas.server;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.client.LVDataClient;

/**
 * A short program to gracefully request the central node to stop.
 * Ctrl-C could do the job, but this one is more flexible and safe.
 */
public class StopDataNode {
    public static void main (String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: java " + StopDataNode.class.getName() + " <conf xml path in classpath> <address of data node to stop(eg: node1:28712)>");
            System.err.println("ex: java " + StopDataNode.class.getName() + " lvfs_conf.xml");
            return;
        }
        ConfFileUtil.addConfFilePath(args[0]);
        
        Configuration conf = new Configuration();
        LVDataClient client = new LVDataClient(conf, args[1]);
        client.getChannel().shutdown();
        Thread.sleep(200L);
        client.release();
        
        System.out.println ("requested to shutdown");
    }
}
