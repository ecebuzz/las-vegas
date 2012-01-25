package edu.brown.lasvegas.server;

import java.io.InputStream;

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
        try {
            InputStream test = StandaloneDataNode.class.getResourceAsStream(args[0]);
            test.read();
            test.close();
        } catch (Exception ex) {
            System.err.println (args[0] + " cannot be read as a resource. Is it in the classpath?");
            return;
        }
        Configuration.addDefaultResource(args[0]);
        boolean clear = args.length >= 2 && new Boolean (args[1]).booleanValue();
        
        Configuration conf = new Configuration();
        LVDataNode dataNode = new LVDataNode(conf, null, true, clear);
        dataNode.start(null);
        dataNode.join();
        
        System.out.println ("exit StandaloneDataNode");
    }
}
