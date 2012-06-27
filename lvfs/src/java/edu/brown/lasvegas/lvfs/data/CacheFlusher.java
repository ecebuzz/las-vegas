package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.lvfs.data.job.DiskCacheFlushJobController;
import edu.brown.lasvegas.lvfs.data.job.DiskCacheFlushJobParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * A program to flush disk cache in data nodes.
 */
public class CacheFlusher {
    private static final Logger LOG = Logger.getLogger(CacheFlusher.class);

    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;
    
    public CacheFlusher (LVMetadataProtocol metaRepo) {
    	this.metaRepo = metaRepo;
    }
    public CacheFlusher (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
    }
    public void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    private DiskCacheFlushJobParameters parseInputFile (String inputFileName) throws Exception {
    	DiskCacheFlushJobParameters params = new DiskCacheFlushJobParameters();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFileName), "UTF-8"));
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            LOG.info("input line:" + line);
            StringTokenizer tokenizer = new StringTokenizer(line, "\t");
            String nodeName = tokenizer.nextToken();
            LVRackNode node = metaRepo.getRackNode(nodeName);
            if (node == null) {
                throw new IllegalArgumentException("node '" + nodeName + "' doesn't exist in metadata repository. have you started the node?");
            }
            String path = tokenizer.nextToken();
            LOG.info("node " + nodeName + ": file=" + path);
            assert (!params.getNodeFilePathMap().containsKey(node.getNodeId()));
            params.getNodeFilePathMap().put(node.getNodeId(), path);
        }
        reader.close();
        return params;
    }
    public void exec (String inputFileName) throws Exception {
    	DiskCacheFlushJobParameters params = parseInputFile(inputFileName);
    	DiskCacheFlushJobController controller = new DiskCacheFlushJobController(metaRepo, 100L, 100L, 100L);
    	LVJob job = controller.startSync(params);
        LOG.info("finished disk cache flush job:" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 2) {
            System.err.println("usage: java " + CacheFlusher.class.getName() + " <metadata repository address> <name of the file that lists files to read>");
            System.err.println("ex: java " + CacheFlusher.class.getName() + " poseidon:28710 inputs_lineitem.txt");
            return;
        }
        String metaRepoAddress = args[0];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        String inputFileName = args[1];
        
        CacheFlusher program = new CacheFlusher(metaRepoAddress);
        try {
            LOG.info("flushing disk caches..");
            long start = System.currentTimeMillis();
            program.exec(inputFileName);
            long end = System.currentTimeMillis();
            LOG.info("done. elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }
}
