package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.ConfFileUtil;
import edu.brown.lasvegas.server.LVDataNode;
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
    /** hardcoded total count of partitions. */
    // private static final int partitionCount = 12;
    /**
     * hardcoded full path of input files in ALL nodes (yes, I'm lazy. but this is just a benchmark..).
     * It should be a partitioned tbl (see SSB/TPCH's dbgen manual. eg: ./dbgen -T l -s 4 -S 1 -C 2).
    */
    // private static final String inputFilePath = "/home/hkimura/workspace/las-vegas/ssb-dbgen/lineorder_s12_p.tbl";
    //private static final int partitionCount = 4;
    //private static final String inputFilePath = "/home/hkimura/workspace/las-vegas/ssb-dbgen/lineorder_s4_p.tbl";

    private static final int partitionCount = 2;
    private static final String inputFilePath = "/home/hkimura/workspace/las-vegas/ssb-dbgen/lineorder_s2_p.tbl";

    // private static final int partitionCount = 3;
    // private static final String inputFilePath = "/home/hkimura/workspace/las-vegas/ssb-dbgen/lineorder_s3_p.tbl";

    private Configuration conf;
    private LVMetadataClient client;
    private LVMetadataProtocol metaRepo;

    private LVRack rack;
    private LVRackNode[] nodes;
    private LVDatabase database;
    private LVTable table;
    private HashMap<String, LVColumn> columns;
    private int[] columnIds;
    private LVReplicaGroup group;
    
    private void setUp () throws IOException {
        conf = new Configuration();
        client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository");
        metaRepo = client.getChannel();

        String rackName = conf.get(LVDataNode.DATA_RACK_NAME_KEY);
        rack = metaRepo.getRack(rackName);
        if (rack == null) {
            throw new IOException ("Rack not defined yet: " + rackName);
        }
        nodes = metaRepo.getAllRackNodes(rack.getRackId());
        if (nodes.length == 0) {
            throw new IOException ("No node defined in this rack: " + rackName);
        }
        
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
    public void exec () throws Exception {
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        for (LVRackNode node : nodes) {
            params.getNodeFilePathMap().put(node.getNodeId(), new String[]{inputFilePath});
        }
        ImportFractureJobController controller = new ImportFractureJobController(metaRepo, 1000L, 1000L, 100L);
        LOG.info("started the import job...");
        LVJob job = controller.startSync(params);
        LOG.info("finished the import job...:" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length == 0) {
            System.err.println("usage: java " + DataImportMultiNodeBenchmark.class.getName() + " <conf xml path in classpath>");
            System.err.println("ex: java -server -Xmx256m " + DataImportMultiNodeBenchmark.class.getName() + " lvfs_conf.xml");
            return;
        }
        ConfFileUtil.addConfFilePath(args[0]);
        DataImportMultiNodeBenchmark program = new DataImportMultiNodeBenchmark();
        program.setUp();
        try {
            LOG.info("started:" + inputFilePath + " (" + (program.nodes.length) + " nodes, " + partitionCount + " partitions)");
            long start = System.currentTimeMillis();
            program.exec();
            long end = System.currentTimeMillis();
            LOG.info("ended:" + inputFilePath + " (" + (program.nodes.length) + " nodes, " + partitionCount + " partitions)" + ". elapsed time=" + (end - start) + "ms");
        } catch (Exception ex) {
            LOG.error("unexpected exception:" + ex.getMessage(), ex);
        } finally {
            program.tearDown();
        }
        LOG.info("exit");
    }
}
