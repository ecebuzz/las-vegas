package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcases for {@link ImportFractureJobController}.
 * This is the end-to-end testcase for data import.
 */
public class ImportFractureJobControllerTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private MasterMetadataRepository masterRepository;
    private LVRack rack;
    private LVRackNode node;
    private LVDatabase database;
    private LVTable table;
    private LVReplicaGroup group1, group2;
    private LVReplicaScheme scheme11, scheme12, scheme21, scheme22;
    private String rootDir;
    private String tmpDir;
    private DataEngine dataEngine;
    private File inputFile;
    private Configuration conf;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder

        rack = masterRepository.createNewRack("rack1");
        node = masterRepository.createNewRackNode(rack, "node1", "node1:12345");
        database = masterRepository.createNewDatabase("db1");

        // table and its scheme for test
        final String[] columnNames = MiniLineorder.getColumnNames();
        table = masterRepository.createNewTable(database.getDatabaseId(), "table1", columnNames, MiniLineorder.getScheme());
        group1 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, null)});
        group2 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "lo_suppkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, null)});
        int[] columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        CompressionType[] compressionScheme1 = MiniLineorder.getDefaultCompressions();
        CompressionType[] compressionScheme2 = new CompressionType[columnIds.length];
        Arrays.fill(compressionScheme2, CompressionType.NONE);
        scheme11 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), columnIds, compressionScheme1);
        scheme12 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "lo_orderdate"), columnIds, compressionScheme2);
        scheme21 = masterRepository.createNewReplicaScheme(group2, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), columnIds, compressionScheme1);
        scheme22 = masterRepository.createNewReplicaScheme(group2, masterRepository.getColumnByName(table.getTableId(), "lo_orderdate"), columnIds, compressionScheme2);

        conf = new Configuration();
        rootDir = "test/node1_lvfs_" + new Random(System.nanoTime()).nextInt();
        tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
        conf.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
        dataEngine = new DataEngine(masterRepository, node.getNodeId(), conf);

        // create the file to load
        inputFile = new File (tmpDir, "mini_lineorder.tbl");
        if (inputFile.exists()) {
            inputFile.delete();
        }
        byte[] bytes = MiniLineorder.getFileBody();
        FileOutputStream out = new FileOutputStream(inputFile);
        out.write(bytes);
        out.flush();
        out.close();
        assert (inputFile.length() == bytes.length);
        
        dataEngine.start();
    }

    @After
    public void tearDown() throws Exception {
        dataEngine.shutdown();
        masterRepository.shutdown();
        if (inputFile != null && inputFile.exists()) {
            inputFile.delete();
        }
    }
/*
    @Test
    public void testStartAsync() {
        fail("Not yet implemented"); // TODO
    }
*/

    @Test
    public void testStartSync() throws Exception {
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node.getNodeId(), new String[]{tmpDir + "/mini_lineorder.tbl"});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 100, 100, 100);
        LVJob job = controller.startSync(params);
        assertEquals (JobStatus.DONE, job.getStatus());
    }

}
