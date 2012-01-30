package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;
import edu.brown.lasvegas.util.ValueRange;

/**
 * A performance benchmark program to test large data import
 * in multiple nodes on top of a real HDFS cluster.
 * This is NOT a testcase.
 * 
 * <p>This one has to run on a real distributed HDFS cluster,
 * so you need to follow the following steps to run this program.
 * There are a few scripts in src/script to help you. But, still
 * many of the steps are inherently manual (or at least you have to adjust
 * something for your environment).
 * </p>
 * 
 * <h2>Step 1</h2>
 * <p>
 * Download and compile the source code in <b>each</b> machine.
 * <pre>
 * git clone git://github.com/hkimura/las-vegas.git
 * cd las-vegas/lvfs
 * ant
 * </pre>
 * </p>
 * 
 * <h2>Step 2</h2>
 * <p>
 * Configure an xml file. Examples are in src/test/lvfs_conf_xxx.xml.
 * At least modify the followings for your environment:
 * lasvegas.server.meta.address,
 * lasvegas.server.data.address,
 * lasvegas.server.data.node_name,
 * lasvegas.server.data.rack_name
 * </p>
 * 
 * <h2>Step 3</h2>
 * <p>
 * Launch the central node in a single machine.
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml -Dformat=true sa-central
 * </pre>
 * -Dformat is the parameter to specify whether to nuke the metadata repository.
 * In order to stop:  
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml sa-central-stop
 * </pre>
 * Alternatively, you can of course put our jar and conf xml in hadoop's folder
 * and launch Hadoop name node, but it's tedious.
 * </p>
 * 
 * <h2>Step 4</h2>
 * <p>
 * Launch the data node in each machine.
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml -Dformat=true sa-data
 * </pre>
 * -Dformat is the parameter to specify whether to nuke the data folder.
 * In order to stop:  
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml -Daddress=poseidon:28712 sa-data-stop
 * </pre>
 * Alternatively, you can of course put our jar and conf xml in hadoop's folder
 * and launch Hadoop data node, but it's tedious.
 * </p>
 * 
 * <h2>Step 5</h2>
 * <p>
 * Generate and place input files. You are gonna define its full path
 * in this program (so far hard-coded, sorry!).
 * </p>
 * 
 * <h2>Step 6</h2>
 * <p>
 * And then, finally launch this class.
 * </p>
 */
public class DataImportMultiNodeBenchmark {
    private static final Logger LOG = Logger.getLogger(DataImportMultiNodeBenchmark.class);
    private static int partitionCount = 0;

    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;
    
    private void setUp (String metadataAddress) throws IOException {
        conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        metaRepo = client.getChannel();
        
        final String dbname = "db1";
        if (metaRepo.getDatabase(dbname) != null) {
            metaRepo.dropDatabase(metaRepo.getDatabase(dbname).getDatabaseId());
            LOG.info("dropped existing database");
        }

        database = metaRepo.createNewDatabase(dbname);
        final String[] columnNames = MiniLineorder.getColumnNames();
        columns = new HashMap<String, LVColumn>();
        table = metaRepo.createNewTable(database.getDatabaseId(), "lineorder", columnNames, MiniLineorder.getScheme());
        for (LVColumn column : metaRepo.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }
        columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = metaRepo.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        ValueRange[] ranges = new ValueRange[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
            ranges[i] = new ValueRange ();
            ranges[i].setType(ColumnType.INTEGER);
            if (i == 0) {
                ranges[i].setStartKey(null);
            } else {
                ranges[i].setStartKey(6000000 * i + 1);
            }
            if (i == partitionCount - 1) {
                ranges[i].setEndKey(null);
            } else {
                ranges[i].setEndKey(6000000 * (i + 1) + 1);
            }
        }
        group = metaRepo.createNewReplicaGroup(table, columns.get("lo_orderkey"), ranges);
        metaRepo.createNewReplicaScheme(group, columns.get("lo_orderkey"), columnIds, MiniLineorder.getDefaultCompressions());
        metaRepo.createNewReplicaScheme(group, columns.get("lo_suppkey"), columnIds, MiniLineorder.getDefaultCompressions());
        metaRepo.createNewReplicaScheme(group, columns.get("lo_orderdate"), columnIds, MiniLineorder.getDefaultCompressions());
    }
    private void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public void exec (ImportFractureJobParameters params) throws Exception {
        ImportFractureJobController controller = new ImportFractureJobController(metaRepo, 1000L, 1000L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
    }
    public ImportFractureJobParameters parseInputFile (String inputFileName) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFileName), "UTF-8"));
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
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
            params.getNodeFilePathMap().put(node.getNodeId(), list.toArray(new String[0]));
        }
        reader.close();
        return params;
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 3) {
            System.err.println("usage: java " + DataImportMultiNodeBenchmark.class.getName() + " <partitionCount> <metadata repository address> <name of the file that lists input files (*)>");
            System.err.println("ex: java " + DataImportMultiNodeBenchmark.class.getName() + " 2 poseidon:28710 inputs.txt");
            System.err.println("(*) the file format is \"<data node name>TAB<input file path 1>TAB<input file path 2>...\". One line for one node. Should be UTF-8 encoded file.");
            System.err.println("ex:\n"
                            + "poseidon    /home/hkimura/workspace/las-vegas/ssb-dbgen/lineorder_s2_p.tbl\n"
                            + "artemis  /home/hkimura/workspace/las-vegas/ssb-dbgen/lineorder_s2_p.tbl");
            // It should be a partitioned tbl (see SSB/TPCH's dbgen manual. eg: ./dbgen -T l -s 4 -S 1 -C 2).
            return;
        }
        partitionCount = Integer.parseInt(args[0]);
        if (partitionCount <= 0) {
            throw new IllegalArgumentException ("invalid partition count :" + args[0]);
        }
        LOG.info("partitionCount=" + partitionCount);
        String metaRepoAddress = args[1];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        String inputFileName = args[2];
        
        DataImportMultiNodeBenchmark program = new DataImportMultiNodeBenchmark();
        program.setUp(metaRepoAddress);
        try {
            ImportFractureJobParameters params = program.parseInputFile (inputFileName); 
            LOG.info("started: " + partitionCount + " partitions)");
            long start = System.currentTimeMillis();
            program.exec(params);
            long end = System.currentTimeMillis();
            LOG.info("ended: " + partitionCount + " partitions). elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }
}
