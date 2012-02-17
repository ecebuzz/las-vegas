package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * A utility program to upload a file to HDFS from partitioned files.
 * This is especially useful for TPC data upload because there is no easy way
 * to construct one file in HDFS from partitioned files.
 * To use this program, start central/data nodes and then call ant task hdfs-multi-import.
 */
public class HdfsMultiFileImporter {
    private static final Logger LOG = Logger.getLogger(HdfsMultiFileImporter.class);

    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;
    
    private ArrayList<InputLine> inputs;
    
    private void setUp (String metadataAddress, String inputFileName) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
        inputs = parseInputFile(inputFileName);
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    private static class InputLine {
        private LVRackNode node;
        private ArrayList<String> paths;
    }
    public ArrayList<InputLine> parseInputFile (String inputFileName) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFileName), "UTF-8"));
        ArrayList<InputLine> inputs = new ArrayList<InputLine>();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            LOG.info("input line:" + line);
            StringTokenizer tokenizer = new StringTokenizer(line, "\t");
            String nodeName = tokenizer.nextToken();
            LVRackNode node = metaRepo.getRackNode(nodeName);
            if (node == null) {
                throw new IllegalArgumentException("node '" + nodeName + "' doesn't exist in metadata repository. have you started the node?");
            }
            ArrayList<String> list = new ArrayList<String>();
            while (tokenizer.hasMoreTokens()) {
                String path = tokenizer.nextToken().trim();
                LOG.info("node " + nodeName + ": file=" + path);
                list.add(path);
            }
            InputLine input = new InputLine();
            input.node = node;
            input.paths = list;
            inputs.add(input);
        }
        reader.close();
        return inputs;
    }
    public void exec (String hdfsPath, int replicationFactor) throws Exception {
        LOG.info("testing if each data file exists...");
        for (InputLine line : inputs) {
            LOG.info("testing data connection to " + line.node.getName() + "...");
            LVDataClient dataClient = new LVDataClient(conf, line.node.getAddress());
            try {
                LOG.info("connected.");
                for (String path : line.paths) {
                    DataNodeFile file = new DataNodeFile(conf, dataClient.getChannel(), path);
                    if (file.exists()) {
                        LOG.info("okay, " + path + " exists and length = " + file.length() + " bytes");
                    } else {
                        throw new IOException ("Oops, " + path + " doesn't exist in " + line.node);
                    }
                }
                LOG.info("okay, all files in " + line.node.getName() + " exist.");
            } finally {
                dataClient.release();
            }
        }
        
        LOG.info("verified every data file exists and is reachable. starting the upload...");
        byte[] buffer = new byte[64 << 20];
        DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.get(URI.create(hdfsPath), conf);
        Path hpath = new Path(hdfsPath);
        FSDataOutputStream out = hdfs.create(hpath, true, 1 << 20, (short) replicationFactor, 64 << 20);
        boolean firstFile = true;
        try {
            LOG.info("writing...");
            long totalBytesWritten = 0;
            for (InputLine line : inputs) {
                LOG.info("connecting to " + line.node.getName() + "...");
                LVDataClient dataClient = new LVDataClient(conf, line.node.getAddress());
                try {
                    LOG.info("connected.");
                    for (String path : line.paths) {
                        LOG.info("copying " + path + " in " + line.node.getName() + "...");
                        if (firstFile) {
                            firstFile = false;
                        } else {
                            // after each file, append a line-feed
                            out.write("\r\n".getBytes("UTF-8"));
                        }
                        DataNodeFile file = new DataNodeFile(conf, dataClient.getChannel(), path);
                        InputStream in = file.getInputStream();
                        try {
                            while (true) {
                                int read = in.read(buffer);
                                if (read < 0) {
                                    break;
                                }
                                out.write(buffer, 0, read);
                                out.hflush();
                                totalBytesWritten += read;
                                LOG.info ("totalBytesWritten=" + totalBytesWritten + "bytes(" + ((double) totalBytesWritten / (1 << 20)) + "MB)");
                            }
                        } finally {
                            in.close();
                        }
                    }
                    LOG.info("okay, copied all files in " + line.node.getName());
                } finally {
                    dataClient.release();
                }
            }
            out.hsync();
        } finally {
            out.close();
        }
        LOG.info("DONE! wrote all blocks.");
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 4) {
            System.err.println("usage: java " + HdfsMultiFileImporter.class.getName() + " <metadata repository address> <name of the file that lists input files> <HDFS path to be created> <replication factor>");
            System.err.println("ex: java " + HdfsMultiFileImporter.class.getName() + " poseidon:28710 inputs_lineitem.txt hdfs://poseidon:9000/tpch/lineitem.tbl 3");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        String inputFileName = args[1];
        LOG.info("inputFileName=" + inputFileName);
        String hdfsPath = args[2];
        LOG.info("hdfsPath=" + hdfsPath);
        if (!hdfsPath.startsWith("hdfs://")) {
            System.err.println("the specified HDFS path doesn't seem valid:" + hdfsPath);
            return;
        }
        int replicationFactor = Integer.parseInt(args[3]);
        LOG.info("replicationFactor=" + replicationFactor);
        
        HdfsMultiFileImporter program = new HdfsMultiFileImporter();
        program.setUp(metaRepoAddress, inputFileName);
        try {
            LOG.info("started");
            long start = System.currentTimeMillis();
            program.exec(hdfsPath, replicationFactor);
            long end = System.currentTimeMillis();
            LOG.info("ended. elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }

}
