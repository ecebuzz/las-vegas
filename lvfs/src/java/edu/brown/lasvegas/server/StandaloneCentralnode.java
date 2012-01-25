package edu.brown.lasvegas.server;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

/**
 * A standalone program to launch centralnode without HDFS name node.
 * Convenient for testing and benchmarks.
 */
public class StandaloneCentralnode {
    public static void main (String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: java " + StandaloneCentralnode.class.getName() + " <conf xml path in classpath>");
            System.err.println("ex: java -server -Xmx1024m " + StandaloneCentralnode.class.getName() + " lvfs_conf.xml");
            return;
        }
        try {
            InputStream test = StandaloneCentralnode.class.getResourceAsStream(args[0]);
            test.read();
            test.close();
        } catch (Exception ex) {
            System.err.println (args[0] + " cannot be read as a resource. Is it in the classpath?");
            return;
        }
        Configuration.addDefaultResource(args[0]);
        
        Configuration conf = new Configuration();
        LVCentralNode centralNode = new LVCentralNode(conf);
        centralNode.start(null);
        centralNode.join();
        
        System.out.println ("exit StandaloneCentralnode");
    }

}
