package edu.brown.lasvegas.server;

import org.apache.hadoop.conf.Configuration;

/**
 * A standalone program to launch centralnode without HDFS name node.
 * Convenient for testing and benchmarks.
 */
public class StandaloneCentralNode {
    public static void main (String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: java " + StandaloneCentralNode.class.getName() + " <conf xml path in classpath> <clear the metadata repository (default:false)>");
            System.err.println("ex: java -server -Xmx1024m " + StandaloneCentralNode.class.getName() + " lvfs_conf.xml true");
            return;
        }
        ConfFileUtil.addConfFilePath(args[0]);
        boolean clear = args.length >= 2 && new Boolean (args[1]).booleanValue();
        
        Configuration conf = new Configuration();
        LVCentralNode centralNode = new LVCentralNode(conf, clear);
        centralNode.start(null);
        centralNode.join();
        
        System.out.println ("exit StandaloneCentralnode");
    }

}
