package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobController;
import edu.brown.lasvegas.lvfs.data.job.ImportFractureJobParameters;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcases for {@link ImportFractureJobController}.
 * This is the end-to-end testcase for data import.
 */
public class ImportFractureJobControllerTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final String DATANODE1_ADDRESS = "localhost:12345";
    private static final String DATANODE2_ADDRESS = "localhost:12346";
    private static final String DATANODE1_NAME = "node1";
    private static final String DATANODE2_NAME = "node2";

    // these are set in setUpBeforeClass
    private static MasterMetadataRepository masterRepository;
    private static LVRack rack1, rack2;
    private static LVRackNode node1, node2; // on rack1, rack2 respectively
    private static LVDatabase database;
    private static String rootDir1, rootDir2;
    private static String tmpDir1, tmpDir2;
    private static LVDataNode dataNode1, dataNode2; // on node1, node2 respectively
    private static File inputFile; // on node1
    private static Configuration conf1, conf2;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder

        rack1 = masterRepository.createNewRack("rack1");
        node1 = masterRepository.createNewRackNode(rack1, DATANODE1_NAME, DATANODE1_ADDRESS);
        rack2 = masterRepository.createNewRack("rack2");
        node2 = masterRepository.createNewRackNode(rack2, DATANODE2_NAME, DATANODE2_ADDRESS);
        assert (node2 != null);
        database = masterRepository.createNewDatabase("db1");

        conf1 = new Configuration();
        rootDir1 = "test/node1_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
        tmpDir1 = rootDir1 + "/tmp";
        conf1.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir1);
        conf1.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir1);
        conf1.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir1);
        conf1.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
        conf1.set(LVDataNode.DATA_ADDRESS_KEY, DATANODE1_ADDRESS);
        conf1.set(LVDataNode.DATA_NODE_NAME_KEY, DATANODE1_NAME);
        conf1.set(LVDataNode.DATA_RACK_NAME_KEY, "rack1");
        dataNode1 = new LVDataNode(conf1, masterRepository);

        conf2 = new Configuration();
        rootDir2 = "test/node2_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
        tmpDir2 = rootDir2 + "/tmp";
        conf2.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir2);
        conf2.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir2);
        conf2.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir2);
        conf2.setLong(DataTaskPollingThread.POLLING_INTERVAL_KEY, 100L);
        conf2.set(LVDataNode.DATA_ADDRESS_KEY, DATANODE2_ADDRESS);
        conf2.set(LVDataNode.DATA_NODE_NAME_KEY, DATANODE2_NAME);
        conf2.set(LVDataNode.DATA_RACK_NAME_KEY, "rack2");
        dataNode2 = new LVDataNode(conf2, masterRepository);

        dataNode1.start(null);
        dataNode2.start(null);
    }
    private static void createInputFile (MiniDataSource dataSource, String filename) throws Exception {
        // create the file to load
        inputFile = new File (tmpDir1, filename);
        if (inputFile.exists()) {
            inputFile.delete();
        }
        if (!inputFile.getParentFile().exists()) {
            inputFile.getParentFile().mkdirs();
        }
        byte[] bytes = dataSource.getFileBody();
        FileOutputStream out = new FileOutputStream(inputFile);
        out.write(bytes);
        out.flush();
        out.close();
        assert (inputFile.length() == bytes.length);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        dataNode1.close();
        dataNode2.close();
        masterRepository.shutdown();
        if (inputFile != null && inputFile.exists()) {
            inputFile.delete();
        }
    }

    @Test
    public void testNoPartitionSSBLineorder() throws Exception {
        MiniDataSource dataSource = new MiniSSBLineorder();
        createInputFile (dataSource, "copied.tbl");
        // table and its scheme for test
        final String[] columnNames = dataSource.getColumnNames();
        LVTable table = masterRepository.createNewTable(database.getDatabaseId(), "tablenopart", columnNames, dataSource.getScheme());
        HashMap<String, LVColumn> columns = new HashMap<String, LVColumn>();
        for (LVColumn column : masterRepository.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }

        LVReplicaGroup group1 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, null)});
        LVReplicaGroup group2 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "lo_suppkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, null)});
        int[] columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        CompressionType[] compressionScheme1 = dataSource.getDefaultCompressions();
        CompressionType[] compressionScheme2 = new CompressionType[columnIds.length];
        Arrays.fill(compressionScheme2, CompressionType.NONE);
        LVReplicaScheme scheme11 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme12 =*/ masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "lo_orderdate"), columnIds, compressionScheme2);
        /*LVReplicaScheme scheme21 =*/ masterRepository.createNewReplicaScheme(group2, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme22 =*/ masterRepository.createNewReplicaScheme(group2, masterRepository.getColumnByName(table.getTableId(), "lo_orderdate"), columnIds, compressionScheme2);

        // let's start!
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node1.getNodeId(), new String[]{tmpDir1 + "/copied.tbl"});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 100, 100, 100);
        LVJob job = controller.startSync(params);
        assertEquals (JobStatus.DONE, job.getStatus());

        // check the contents of imported files
        LVFracture fracture = controller.getFracture();
        LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(scheme11.getSchemeId(), fracture.getFractureId());
        LVReplicaPartition partition = masterRepository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), 0);
        LVColumnFile columnFile = masterRepository.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), columns.get("lo_orderkey").getColumnId());
        ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
        ColumnFileReaderBundle readers = new ColumnFileReaderBundle(bundle);
        @SuppressWarnings("unchecked")
        TypedReader<Integer, int[]> dataReader = (TypedReader<Integer, int[]>) readers.getDataReader();
        assertEquals(dataSource.getCount(), dataReader.getTotalTuples());
        int[] values = new int[dataReader.getTotalTuples()];
        assertEquals(values.length, dataReader.readValues(values, 0, values.length));
        assertEquals(1, values[0]);
        for (int i = 1; i < values.length; ++i) {
            assertTrue(values[i] >= values[i - 1]);
        }
        readers.close();
    }

    @Test
    public void testPartitionSSBLineorder() throws Exception {
        MiniDataSource dataSource = new MiniSSBLineorder();
        createInputFile (dataSource, "copied.tbl");
        // table and its scheme for test
        final String[] columnNames = dataSource.getColumnNames();
        LVTable table = masterRepository.createNewTable(database.getDatabaseId(), "tablepart", columnNames, dataSource.getScheme());
        HashMap<String, LVColumn> columns = new HashMap<String, LVColumn>();
        for (LVColumn column : masterRepository.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }

        LVReplicaGroup group1 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, 10), new ValueRange(ColumnType.INTEGER, 10, null)});
        int[] columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        CompressionType[] compressionScheme1 = dataSource.getDefaultCompressions();
        CompressionType[] compressionScheme2 = new CompressionType[columnIds.length];
        Arrays.fill(compressionScheme2, CompressionType.NONE);
        LVReplicaScheme scheme11 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "lo_orderkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme12 =*/ masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "lo_orderdate"), columnIds, compressionScheme2);

        // let's start!
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node1.getNodeId(), new String[]{tmpDir1 + "/copied.tbl"});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 100, 100, 100);
        LVJob job = controller.startSync(params);
        assertEquals (JobStatus.DONE, job.getStatus());

        // check the contents of imported files
        LVFracture fracture = controller.getFracture();
        int[] counts = new int[]{23, dataSource.getCount() - 23};
        for (int par = 0; par < 2; ++par) {
            LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(scheme11.getSchemeId(), fracture.getFractureId());
            LVReplicaPartition partition = masterRepository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), par);
            LVColumnFile columnFile = masterRepository.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), columns.get("lo_orderkey").getColumnId());
            ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
            ColumnFileReaderBundle readers = new ColumnFileReaderBundle(bundle);
            @SuppressWarnings("unchecked")
            TypedReader<Integer, int[]> dataReader = (TypedReader<Integer, int[]>) readers.getDataReader();
            assertEquals(counts[par], dataReader.getTotalTuples());
            int[] values = new int[dataReader.getTotalTuples()];
            assertEquals(values.length, dataReader.readValues(values, 0, values.length));
            for (int i = 0; i < values.length; ++i) {
                if (i > 0) {
                    assertTrue(values[i] >= values[i - 1]);
                }
                if (par == 0) {
                    assertTrue(values[i] < 10);
                } else {
                    assertTrue(values[i] >= 10);
                }
            }
            readers.close();
        }
    }

    @Test
    public void testNoPartitionTPCHLineitem() throws Exception {
        MiniDataSource dataSource = new MiniTPCHLineitem();
        createInputFile (dataSource, "copied.tbl");
        // table and its scheme for test
        final String[] columnNames = dataSource.getColumnNames();
        LVTable table = masterRepository.createNewTable(database.getDatabaseId(), "tpchnopart", columnNames, dataSource.getScheme());
        HashMap<String, LVColumn> columns = new HashMap<String, LVColumn>();
        for (LVColumn column : masterRepository.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }

        LVReplicaGroup group1 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "l_orderkey"), new ValueRange[]{new ValueRange(ColumnType.BIGINT, null, null)});
        LVReplicaGroup group2 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "l_suppkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, null)});
        int[] columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        CompressionType[] compressionScheme1 = dataSource.getDefaultCompressions();
        CompressionType[] compressionScheme2 = new CompressionType[columnIds.length];
        Arrays.fill(compressionScheme2, CompressionType.NONE);
        LVReplicaScheme scheme11 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "l_orderkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme12 =*/ masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "l_commitdate"), columnIds, compressionScheme2);
        /*LVReplicaScheme scheme21 =*/ masterRepository.createNewReplicaScheme(group2, masterRepository.getColumnByName(table.getTableId(), "l_orderkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme22 =*/ masterRepository.createNewReplicaScheme(group2, masterRepository.getColumnByName(table.getTableId(), "l_commitdate"), columnIds, compressionScheme2);

        // let's start!
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node1.getNodeId(), new String[]{tmpDir1 + "/copied.tbl"});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 100, 100, 100);
        LVJob job = controller.startSync(params);
        assertEquals (JobStatus.DONE, job.getStatus());

        // check the contents of imported files
        LVFracture fracture = controller.getFracture();
        LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(scheme11.getSchemeId(), fracture.getFractureId());
        LVReplicaPartition partition = masterRepository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), 0);
        LVColumnFile columnFile = masterRepository.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), columns.get("l_orderkey").getColumnId());
        ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
        ColumnFileReaderBundle readers = new ColumnFileReaderBundle(bundle);
        @SuppressWarnings("unchecked")
        TypedReader<Long, long[]> dataReader = (TypedReader<Long, long[]>) readers.getDataReader();
        assertEquals(dataSource.getCount(), dataReader.getTotalTuples());
        long[] values = new long[dataReader.getTotalTuples()];
        assertEquals(values.length, dataReader.readValues(values, 0, values.length));
        assertEquals(1, values[0]);
        for (int i = 1; i < values.length; ++i) {
            assertTrue(values[i] >= values[i - 1]);
        }
        readers.close();
    }

    @Test
    public void testSuppPartitionTPCHLineitem() throws Exception {
        MiniDataSource dataSource = new MiniTPCHLineitem();
        createInputFile (dataSource, "copied.tbl");
        // table and its scheme for test
        final String[] columnNames = dataSource.getColumnNames();
        LVTable table = masterRepository.createNewTable(database.getDatabaseId(), "tpchtablepart", columnNames, dataSource.getScheme());
        HashMap<String, LVColumn> columns = new HashMap<String, LVColumn>();
        for (LVColumn column : masterRepository.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }

        LVReplicaGroup group1 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "l_suppkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, 5000), new ValueRange(ColumnType.INTEGER, 5000, null)});
        int[] columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        CompressionType[] compressionScheme1 = dataSource.getDefaultCompressions();
        CompressionType[] compressionScheme2 = new CompressionType[columnIds.length];
        Arrays.fill(compressionScheme2, CompressionType.NONE);
        LVReplicaScheme scheme11 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "l_suppkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme12 =*/ masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "l_orderkey"), columnIds, compressionScheme2);

        // let's start!
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node1.getNodeId(), new String[]{tmpDir1 + "/copied.tbl"});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 100, 100, 100);
        LVJob job = controller.startSync(params);
        assertEquals (JobStatus.DONE, job.getStatus());

        // check the contents of imported files
        LVFracture fracture = controller.getFracture();
        int[] counts = new int[]{30, dataSource.getCount() - 30};
        for (int par = 0; par < 2; ++par) {
            LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(scheme11.getSchemeId(), fracture.getFractureId());
            LVReplicaPartition partition = masterRepository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), par);
            LVColumnFile columnFile = masterRepository.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), columns.get("l_suppkey").getColumnId());
            ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
            ColumnFileReaderBundle readers = new ColumnFileReaderBundle(bundle);
            @SuppressWarnings("unchecked")
            TypedReader<Integer, int[]> dataReader = (TypedReader<Integer, int[]>) readers.getDataReader();
            assertEquals(counts[par], dataReader.getTotalTuples());
            int[] values = new int[dataReader.getTotalTuples()];
            assertEquals(values.length, dataReader.readValues(values, 0, values.length));
            for (int i = 0; i < values.length; ++i) {
                if (i > 0) {
                    assertTrue(values[i] >= values[i - 1]);
                }
                if (par == 0) {
                    assertTrue(values[i] < 5000);
                } else {
                    assertTrue(values[i] >= 5000);
                }
            }
            readers.close();
        }
    }

    @Test
    public void testNoPartitionTPCHPart() throws Exception {
        MiniDataSource dataSource = new MiniTPCHPart();
        createInputFile (dataSource, "copied.tbl");
        // table and its scheme for test
        final String[] columnNames = dataSource.getColumnNames();
        LVTable table = masterRepository.createNewTable(database.getDatabaseId(), "tpchnopartpart", columnNames, dataSource.getScheme());
        HashMap<String, LVColumn> columns = new HashMap<String, LVColumn>();
        for (LVColumn column : masterRepository.getAllColumnsExceptEpochColumn(table.getTableId())) {
            columns.put(column.getName(), column);
        }

        LVReplicaGroup group1 = masterRepository.createNewReplicaGroup(table, masterRepository.getColumnByName(table.getTableId(), "p_partkey"), new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, null)});
        int[] columnIds = new int[columnNames.length];
        for (int i = 0; i < columnIds.length; ++i) {
            columnIds[i] = masterRepository.getColumnByName(table.getTableId(), columnNames[i]).getColumnId();
        }
        CompressionType[] compressionScheme1 = dataSource.getDefaultCompressions();
        CompressionType[] compressionScheme2 = new CompressionType[compressionScheme1.length];
        Arrays.fill(compressionScheme2, CompressionType.NONE);
        LVReplicaScheme scheme11 = masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "p_partkey"), columnIds, compressionScheme1);
        /*LVReplicaScheme scheme12 =*/ masterRepository.createNewReplicaScheme(group1, masterRepository.getColumnByName(table.getTableId(), "p_size"), columnIds, compressionScheme2);

        // let's start!
        ImportFractureJobParameters params = new ImportFractureJobParameters(table.getTableId());
        params.getNodeFilePathMap().put(node1.getNodeId(), new String[]{tmpDir1 + "/copied.tbl"});
        ImportFractureJobController controller = new ImportFractureJobController(masterRepository, 100, 100, 100);
        LVJob job = controller.startSync(params);
        assertEquals (JobStatus.DONE, job.getStatus());

        // check the contents of imported files
        LVFracture fracture = controller.getFracture();
        LVReplica replica = masterRepository.getReplicaFromSchemeAndFracture(scheme11.getSchemeId(), fracture.getFractureId());
        LVReplicaPartition partition = masterRepository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), 0);
        LVColumnFile columnFile = masterRepository.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), columns.get("p_partkey").getColumnId());
        ColumnFileBundle bundle = new ColumnFileBundle(columnFile);
        ColumnFileReaderBundle readers = new ColumnFileReaderBundle(bundle);
        @SuppressWarnings("unchecked")
        TypedReader<Integer, int[]> dataReader = (TypedReader<Integer, int[]>) readers.getDataReader();
        assertEquals(dataSource.getCount(), dataReader.getTotalTuples());
        int[] values = new int[dataReader.getTotalTuples()];
        assertEquals(values.length, dataReader.readValues(values, 0, values.length));
        for (int i = 0; i < values.length; ++i) {
            assertEquals(i + 1, values[i]);
        }
        readers.close();
    }
}
