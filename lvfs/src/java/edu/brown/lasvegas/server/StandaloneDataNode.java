package edu.brown.lasvegas.server;

import org.apache.hadoop.conf.Configuration;

/**
 * A standalone program to launch datanode without HDFS data node.
 * Convenient for testing and benchmarks.
 */
public class StandaloneDataNode {
    public static void main (String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: java " + StandaloneDataNode.class.getName() + " <conf xml path in classpath> <clear the data folder (default:false)>");
            System.err.println("ex: java -server -Xmx1024m " + StandaloneDataNode.class.getName() + " lvfs_conf.xml true");
            return;
        }
        ConfFileUtil.addConfFilePath(args[0]);
        boolean clear = args.length >= 2 && new Boolean (args[1]).booleanValue();
        
        Configuration conf = new Configuration();
        System.out.println("address of central node:" + conf.get(LVCentralNode.METAREPO_ADDRESS_KEY));
        LVDataNode dataNode = new LVDataNode(conf, null, true, clear);
        dataNode.start(null);
        dataNode.join();
        
        System.out.println ("exit StandaloneDataNode");
    }
}
