package edu.brown.lasvegas.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * A short program to register a list of data nodes so that
 * their IDs are ordered by the pre-set list.
 * This is useful to optimize the assignment of partitions on each node
 * if the user knows where the data file is and which range of data it contains.
 * 
 * For example, TPC-H's dbgen generates data files partitioned by the table's primary key.
 * For replica groups that are partitioned by primary keys, ordering data node's IDs by the
 * dbgen's "-S" parameter given to the node minimizes network communications while data loading.
 * Without this program, data node's IDs would be ordered randomly
 * (ordered by the time when each node's first request arrives the central node, i.e., race condition).
 */
public class PreregisterDataNodes {
    /** one line in a datanode list file. */
    private static class DatanodeEntry {
        DatanodeEntry (String line) {
            StringTokenizer tokenizer = new StringTokenizer(line, "\t");
            nodeName = tokenizer.nextToken().trim();
            rackName = tokenizer.nextToken().trim();
            assert (nodeName.length() > 0);
            assert (rackName.length() > 0);
        }
        String nodeName;
        String rackName;
    }
    private static List<DatanodeEntry> readListFile (String filepath) throws IOException {
        List<DatanodeEntry> list = new ArrayList<DatanodeEntry>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filepath), "UTF-8"));
        try {
            HashSet<String> nodeNames = new HashSet<String>();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.trim().length() == 0) {
                    continue;
                }
                DatanodeEntry entry = new DatanodeEntry(line);
                list.add(entry);
                if (nodeNames.contains(entry.nodeName)) {
                    throw new IOException ("this node name appeared more than once: " + entry.nodeName);
                }
                nodeNames.add(entry.nodeName);
            }
        } finally {
            reader.close();
        }
        return list;
    }
    
    /** this is public just for testcase. */
    public static void execute (LVMetadataProtocol metaRepo, String datanodeListFilePath) throws IOException {
        List<DatanodeEntry> entries = readListFile (datanodeListFilePath);
        for (DatanodeEntry entry : entries) {
            String rackName = entry.rackName;
            LVRack rack = metaRepo.getRack(rackName);
            if (rack == null) {
                System.out.println("creating LVRack:" + rackName);
                rack = metaRepo.createNewRack(rackName);
            }

            String nodeName = entry.nodeName;
            LVRackNode node = metaRepo.getRackNode(nodeName);
            
            if (node == null) {
                System.out.println("creating LVRackNode:" + nodeName);
                node = metaRepo.createNewRackNode(rack, nodeName, nodeName + ":28712");
                // set its status to be lost. this will be updated when the node really starts up.
                metaRepo.updateRackNodeStatusNoReturn(node.getNodeId(), RackNodeStatus.LOST);
            } else {
                System.out.println ("this node already exists:" + nodeName);
                if (node.getRackId() != rack.getRackId()) {
                    throw new IOException ("This node doesn't belong to the specified rack:" + node + " : " + rackName);
                }
            }
            System.out.println("Result:" + node);
        }
    }
    
    public static void main (String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: java " + PreregisterDataNodes.class.getName() + " <conf xml path in classpath> <datanode list file>");
            System.err.println("ex: java " + PreregisterDataNodes.class.getName() + " lvfs_conf.xml datanodes_example.txt");
            System.err.println("syntax of datanode list file: <Node name>TAB<Rack name>LINEFEED");
            return;
        }
        ConfFileUtil.addConfFilePath(args[0]);
        
        Configuration conf = new Configuration();
        LVMetadataClient client = new LVMetadataClient(conf);
        try {
            LVMetadataProtocol metaRepo = client.getChannel();
            execute(metaRepo, args[1]);
        } finally {
            client.release();
        }
        
        System.out.println ("pre-registered data nodes");
    }

}
