package edu.brown.lasvegas.lvfs.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import edu.brown.lasvegas.ColumnStatus;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVObjectType;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackAssignment;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVSubPartitionScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.RackStatus;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.TableStatus;
import edu.brown.lasvegas.protocol.MetadataProtocol;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Base class of testcases for Metadata repository.
 * As each implementation should behave in a same way,
 * most of tests are defined here regardless the test is for
 * master or slave.
 * 
 * Name of this abstract class doesn't end with Test so that our ant script
 * would skip this. 
 */
public abstract class MetadataRepositoryTestBase {
    /**
     * The derived testcase sets the repository instance to be tested.
     */
    protected MetadataProtocol repository;
    
    /**
     * Flushes the tested repository, closes it and then reloads it.
     * Used to test durability of the repository.
     * @throws IOException
     */
    protected abstract void reloadRepository() throws IOException;

    /** called from setUp() in derived class. */
    protected void baseSetUp(MetadataProtocol repository) throws Exception {
        this.repository = repository;
        initDefaultTestObjects ();
    }

    /** called from tearDown() in derived class. */
    protected void baseTearDown() throws Exception {
        repository = null;
    }
    
    
    private final static String DEFAULT_TABLE_NAME = "deftable";
    private final static String DEFAULT_RACK_NAME = "default_rack";
    private final static String DEFAULT_RACK_NODE_NAME = "default_node.default_rack.dummy.org";
    private LVTable DEFAULT_TABLE;
    /** 0=epoch, 1=intcol, 2=strcol, 3=floatcol, 4=tscol. */
    private LVColumn[] DEFAULT_COLUMNS;
    private LVFracture DEFAULT_FRACTURE;
    private LVRack DEFAULT_RACK;
    private LVRackNode DEFAULT_RACK_NODE;
    private LVRackAssignment DEFAULT_RACK_ASSIGNMENT;
    /** intcol partition. */
    private LVReplicaGroup DEFAULT_GROUP;
    /** intcol sort. */
    private LVReplicaScheme DEFAULT_SCHEME;
    /** 2 partitions (40-140, 140-300). */
    private LVSubPartitionScheme DEFAULT_SUB_PARTITION_SCHEME;
    private LVReplica DEFAULT_REPLICA;
    /** 2 partitions. */
    private LVReplicaPartition[] DEFAULT_REPLICA_PARTITIONS;
    /** [2 partition][5 columns]. */
    private LVColumnFile[][] DEFAULT_COLUMN_FILES;
    
    /** for ease of testing, create a few default objects. */
    private void initDefaultTestObjects () throws IOException {
        for (LVTable existing : repository.getAllTables()) {
            repository.dropTable(existing);
        }
        for (LVRack existing : repository.getAllRacks()) {
            repository.dropRack(existing);
        }
        DEFAULT_TABLE = repository.createNewTable(DEFAULT_TABLE_NAME, new LVColumn[]{
            new LVColumn("intcol", ColumnType.INTEGER),
            new LVColumn("strcol", ColumnType.VARCHAR),
            new LVColumn("floatcol", ColumnType.FLOAT),
            new LVColumn("tscol", ColumnType.TIMESTAMP),
        });
        assertTrue (DEFAULT_TABLE.getTableId() != 0);
        
        DEFAULT_RACK = repository.createNewRack(DEFAULT_RACK_NAME);
        assertTrue (DEFAULT_RACK.getRackId() != 0);
        
        DEFAULT_RACK_NODE = repository.createNewRackNode(DEFAULT_RACK, DEFAULT_RACK_NODE_NAME);
        assertTrue (DEFAULT_RACK_NODE.getNodeId() != 0);
        assertEquals(DEFAULT_RACK.getRackId(), DEFAULT_RACK_NODE.getRackId());
        
        DEFAULT_COLUMNS = repository.getAllColumns(DEFAULT_TABLE.getTableId());
        assertEquals (4 + 1, DEFAULT_COLUMNS.length);

        DEFAULT_FRACTURE = repository.createNewFracture(DEFAULT_TABLE);
        assertTrue (DEFAULT_FRACTURE.getFractureId() > 0);
        assertEquals(DEFAULT_TABLE.getTableId(), DEFAULT_FRACTURE.getTableId());
        
        DEFAULT_FRACTURE.setTupleCount(1000000L);
        DEFAULT_FRACTURE.setRange(new ValueRange<Integer>(0, 100));
        repository.finalizeFracture(DEFAULT_FRACTURE);

        DEFAULT_GROUP = repository.createNewReplicaGroup(DEFAULT_TABLE, DEFAULT_COLUMNS[1]);
        assertTrue (DEFAULT_GROUP.getGroupId() > 0);
        assertEquals(DEFAULT_TABLE.getTableId(), DEFAULT_GROUP.getTableId());
        assertEquals(DEFAULT_COLUMNS[1].getColumnId(), DEFAULT_GROUP.getPartitioningColumnId());
        
        DEFAULT_RACK_ASSIGNMENT = repository.createNewRackAssignment(DEFAULT_RACK, DEFAULT_FRACTURE, DEFAULT_GROUP);
        
        DEFAULT_SUB_PARTITION_SCHEME = repository.createNewSubPartitionScheme(DEFAULT_FRACTURE, DEFAULT_GROUP);
        assertTrue (DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId() > 0);
        assertEquals(DEFAULT_FRACTURE.getFractureId(), DEFAULT_SUB_PARTITION_SCHEME.getFractureId());
        assertEquals(DEFAULT_GROUP.getGroupId(), DEFAULT_SUB_PARTITION_SCHEME.getGroupId());
        DEFAULT_SUB_PARTITION_SCHEME.setRanges(new ValueRange<?>[]{new ValueRange<Integer>(40, 140), new ValueRange<Integer>(140, 300)});
        repository.finalizeSubPartitionScheme(DEFAULT_SUB_PARTITION_SCHEME);

        HashMap<Integer, CompressionType> comp = new HashMap<Integer, CompressionType>();
        comp.put(DEFAULT_COLUMNS[0].getColumnId(), CompressionType.RLE);
        comp.put(DEFAULT_COLUMNS[1].getColumnId(), CompressionType.NULL_SUPPRESS);
        comp.put(DEFAULT_COLUMNS[2].getColumnId(), CompressionType.DICTIONARY);
        comp.put(DEFAULT_COLUMNS[3].getColumnId(), CompressionType.SNAPPY);
        DEFAULT_SCHEME = repository.createNewReplicaScheme(DEFAULT_GROUP, DEFAULT_COLUMNS[1], comp);
        assertTrue (DEFAULT_SCHEME.getSchemeId() > 0);
        
        DEFAULT_REPLICA = repository.createNewReplica(DEFAULT_SCHEME, DEFAULT_FRACTURE);
        assertTrue (DEFAULT_REPLICA.getReplicaId() > 0);
        assertEquals(DEFAULT_SCHEME.getSchemeId(), DEFAULT_REPLICA.getSchemeId());
        assertEquals(DEFAULT_FRACTURE.getFractureId(), DEFAULT_REPLICA.getFractureId());
        
        DEFAULT_REPLICA_PARTITIONS = new LVReplicaPartition[2];
        DEFAULT_COLUMN_FILES = new LVColumnFile[2][];
        for (int i = 0; i < 2; ++i) {
            DEFAULT_REPLICA_PARTITIONS[i] = repository.createNewReplicaPartition(DEFAULT_REPLICA, i);
            assertEquals(DEFAULT_REPLICA.getReplicaId(), DEFAULT_REPLICA_PARTITIONS[i].getReplicaId());
            assertEquals(i, DEFAULT_REPLICA_PARTITIONS[i].getRange());
            assertEquals(DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId(), DEFAULT_REPLICA_PARTITIONS[i].getSubPartitionSchemeId());
            assertEquals(ReplicaPartitionStatus.BEING_RECOVERED, DEFAULT_REPLICA_PARTITIONS[i].getStatus());
            DEFAULT_REPLICA_PARTITIONS[i] = repository.updateReplicaPartition(DEFAULT_REPLICA_PARTITIONS[i], ReplicaPartitionStatus.OK, DEFAULT_RACK_NODE);
            assertEquals(ReplicaPartitionStatus.OK, DEFAULT_REPLICA_PARTITIONS[i].getStatus());
            DEFAULT_COLUMN_FILES[i] = new LVColumnFile[DEFAULT_COLUMNS.length];
            for (int j = 0; j < DEFAULT_COLUMNS.length; ++j) {
                DEFAULT_COLUMN_FILES[i][j] = repository.createNewColumnFile(DEFAULT_REPLICA_PARTITIONS[i],
                                DEFAULT_COLUMNS[j], "hdfs://dummy_url_" + i + "/colfile" + j, 123456L + j, 654321 + j);
                assertEquals(DEFAULT_COLUMNS[j].getColumnId(), DEFAULT_COLUMN_FILES[i][j].getColumnId());
                assertEquals(123456L + j, DEFAULT_COLUMN_FILES[i][j].getFileSize());
                assertEquals(654321 + j, DEFAULT_COLUMN_FILES[i][j].getChecksum());
                assertEquals(DEFAULT_REPLICA_PARTITIONS[i].getPartitionId(), DEFAULT_COLUMN_FILES[i][j].getPartitionId());
                assertEquals("hdfs://dummy_url_" + i + "/colfile" + j, DEFAULT_COLUMN_FILES[i][j].getHdfsFilePath());
            }
        }
    }
    

    @Test
    public void testIssueNewEpoch() throws IOException {
        int epoch1 = repository.issueNewEpoch();
        int epoch2 = repository.issueNewEpoch();
        assertEquals (epoch1 + 1, epoch2);
        int epoch3 = repository.issueNewEpoch();
        assertEquals (epoch1 + 2, epoch3);

        reloadRepository();

        int epoch4 = repository.issueNewEpoch();
        assertEquals (epoch1 + 3, epoch4);
        int epoch5 = repository.issueNewEpoch();
        assertEquals (epoch1 + 4, epoch5);
    }

    @Test
    public void testIssueNewId() throws IOException {
        int table1 = repository.issueNewId(LVObjectType.TABLE.ordinal());
        int fracture1 = repository.issueNewId(LVObjectType.FRACTURE.ordinal());
        int table2 = repository.issueNewId(LVObjectType.TABLE.ordinal());
        int fracture2 = repository.issueNewId(LVObjectType.FRACTURE.ordinal());
        int table3 = repository.issueNewId(LVObjectType.TABLE.ordinal());
        assertEquals (table1 + 1, table2);
        assertEquals (table1 + 2, table3);
        assertEquals (fracture1 + 1, fracture2);

        reloadRepository();

        int fracture3 = repository.issueNewId(LVObjectType.FRACTURE.ordinal());
        int fracture4 = repository.issueNewId(LVObjectType.FRACTURE.ordinal());
        int table4 = repository.issueNewId(LVObjectType.TABLE.ordinal());

        assertEquals (table1 + 3, table4);
        assertEquals (fracture1 + 2, fracture3);
        assertEquals (fracture1 + 3, fracture4);
    }
    @Test
    public void testIssueNewIdBlock() throws IOException {
        // call issueNewIdBlock with sorta random number (but deterministic) of block size for each of them.
        // this also makes the following test more robust, randomizing the sequential IDs.
        LVObjectType[] array = LVObjectType.values();
        for (int i = 0; i < array.length; ++i) {
            int count = (((i + 17) * 613287413) % (1 << 16));
            if (count < 0) count = -count;
            if (count == 0) count = 1;
            int newId = repository.issueNewIdBlock(array[i].ordinal(), count);
            int next = repository.issueNewId(array[i].ordinal());
            assertEquals (newId + count, next);
        }
    }

    @Test
    public void testSync() throws IOException {
        // other tests indirectly use this method anyway.
        // so this should be enough
        repository.sync();
    }

    @Test
    public void testCheckpoint() throws IOException {
        // not quite in-detail at all, but it's hard to really test this method
        repository.checkpoint();
    }

    // close() is indirectly called in many tests.
    // and calling close() here will cause a trouble due to double-closing.
    // so, let's not test it here..
    // public void testClose() throws IOException {}

    @Test
    public void testCreateGetNewTable() throws IOException {
        int id1, id2;
        {
            LVTable table = repository.createNewTable("aabc", new LVColumn[]{
                new LVColumn("col1", ColumnType.INTEGER),
                new LVColumn("col2", ColumnType.VARCHAR),
                new LVColumn("col3", ColumnType.FLOAT),
            });
            assertTrue (table.getTableId() > 0);
            assertEquals("aabc", table.getName());
            LVColumn[] columns = repository.getAllColumns(table.getTableId());
            assertEquals (3 + 1, columns.length);
            assertEquals (LVColumn.EPOCH_COLUMN_NAME, columns[0].getName());
            assertEquals (columns[0].getColumnId(), table.getFracturingColumnId());
            
            LVTable tableRep = repository.getTable(table.getTableId());
            assertEquals(table.getTableId(), tableRep.getTableId());
            assertEquals("aabc", tableRep.getName());
            assertEquals(columns[0].getColumnId(), tableRep.getFracturingColumnId());
            assertEquals(TableStatus.OK, tableRep.getStatus());
            id1 = table.getTableId();
        }
        {
            LVTable table = repository.createNewTable("aabc2", new LVColumn[]{
                new LVColumn("col1", ColumnType.INTEGER, true),
                new LVColumn("col2", ColumnType.VARCHAR),
                new LVColumn("col3", ColumnType.FLOAT),
            });
            assertTrue (table.getTableId() > 0);
            assertTrue (table.getTableId() != id1);
            assertEquals("aabc2", table.getName());
            LVColumn[] columns = repository.getAllColumns(table.getTableId());
            assertEquals (3 + 1, columns.length);
            assertEquals (LVColumn.EPOCH_COLUMN_NAME, columns[0].getName());
            assertEquals (columns[1].getColumnId(), table.getFracturingColumnId());
            
            LVTable tableRep = repository.getTable(table.getTableId());
            assertEquals(table.getTableId(), tableRep.getTableId());
            assertEquals("aabc2", tableRep.getName());
            assertEquals(columns[1].getColumnId(), tableRep.getFracturingColumnId());
            assertEquals(TableStatus.OK, tableRep.getStatus());
            id2 = table.getTableId();
        }
        
        reloadRepository();
        // test getTable/getAllColumns after reload
        {
            LVTable table = repository.getTable(id1);
            assertEquals (id1, table.getTableId());
            assertEquals("aabc", table.getName());
            assertEquals(TableStatus.OK, table.getStatus());

            LVColumn[] columns = repository.getAllColumns(id1);
            assertEquals (3 + 1, columns.length);
            assertEquals (LVColumn.EPOCH_COLUMN_NAME, columns[0].getName());
            assertEquals (columns[0].getColumnId(), table.getFracturingColumnId());
        }
        {
            LVTable table = repository.getTable(id2);
            assertEquals (id2, table.getTableId());
            assertEquals("aabc2", table.getName());
            assertEquals(TableStatus.OK, table.getStatus());

            LVColumn[] columns = repository.getAllColumns(id2);
            assertEquals (3 + 1, columns.length);
            assertEquals (LVColumn.EPOCH_COLUMN_NAME, columns[0].getName());
            assertEquals (columns[1].getColumnId(), table.getFracturingColumnId());
        }
    }

    @Test
    public void testRequestDropTable() throws IOException {
        assertEquals (TableStatus.OK, DEFAULT_TABLE.getStatus());
        repository.requestDropTable(DEFAULT_TABLE);
        LVTable modified = repository.getTable(DEFAULT_TABLE.getTableId());
        assertEquals (TableStatus.BEING_DROPPED, modified.getStatus());
    }

    @Test
    public void testDropTable() throws IOException {
        assertNotNull(repository.getTable(DEFAULT_TABLE.getTableId()));
        repository.dropTable(DEFAULT_TABLE);
        assertNull(repository.getTable(DEFAULT_TABLE.getTableId()));
        // also, its subsequent objects should be deleted too
        assertEquals(0, repository.getAllColumns(DEFAULT_TABLE.getTableId()).length);
        reloadRepository();
        assertNull(repository.getTable(DEFAULT_TABLE.getTableId()));
        assertEquals(0, repository.getAllColumns(DEFAULT_TABLE.getTableId()).length);
    }

    @Test
    public void testColumnsAssorted() throws IOException {
        int colid;
        {
            LVColumn column = repository.createNewColumn(DEFAULT_TABLE, "newcol", ColumnType.BIGINT);
            validateNewColumn(column);
            colid = column.getColumnId();
        }
        {
            LVColumn column = repository.getColumn(colid);
            validateNewColumn(column);
            LVColumn[] columns = repository.getAllColumns(DEFAULT_TABLE.getTableId());
            assertEquals (DEFAULT_COLUMNS.length + 1, columns.length);
            validateNewColumn(columns[columns.length - 1]);
        }
        
        reloadRepository();
        LVColumn column = repository.getColumn(colid);
        validateNewColumn(column);
        LVColumn[] columns = repository.getAllColumns(DEFAULT_TABLE.getTableId());
        assertEquals (DEFAULT_COLUMNS.length + 1, columns.length);
        validateNewColumn(columns[columns.length - 1]);
    }
    private void validateNewColumn(LVColumn column) {
        assertEquals("newcol", column.getName());
        assertTrue(column.getColumnId() > 0);
        assertEquals(DEFAULT_TABLE.getTableId(), column.getTableId());
        assertEquals(ColumnType.BIGINT, column.getType());
        assertEquals(false, column.isFracturingColumn());
        assertEquals(DEFAULT_COLUMNS.length, column.getOrder());
        assertEquals(ColumnStatus.BEING_CREATED, column.getStatus());
    }

    @Test
    public void testRequestDropColumn() throws IOException {
        assertEquals (ColumnStatus.OK, DEFAULT_COLUMNS[1].getStatus());
        repository.requestDropColumn(DEFAULT_COLUMNS[1]);
        LVColumn modified = repository.getColumn(DEFAULT_COLUMNS[1].getColumnId());
        assertEquals (ColumnStatus.BEING_DROPPED, modified.getStatus());
    }

    @Test
    public void testDropColumn() throws IOException {
        assertNotNull(repository.getColumn(DEFAULT_COLUMNS[1].getColumnId()));
        repository.dropColumn(DEFAULT_COLUMNS[1]);
        assertNull(repository.getColumn(DEFAULT_COLUMNS[1].getColumnId()));
        reloadRepository();
        assertNull(repository.getColumn(DEFAULT_COLUMNS[1].getColumnId()));
    }

    @Test
    public void testFractureAssorted() throws IOException {
        int fractureId1, fractureId2;
        {
            LVFracture fracture = repository.createNewFracture(DEFAULT_TABLE);
            assertTrue (fracture.getFractureId() > 0);
            assertEquals(DEFAULT_TABLE.getTableId(), fracture.getTableId());
            
            fracture.setTupleCount(123456789L);
            fracture.setRange(new ValueRange<Integer>(100, 300));
            repository.finalizeFracture(fracture);
            fractureId1 = fracture.getFractureId();
        }
        validateFracture (repository.getFracture(fractureId1), fractureId1, 123456789L, 100, 300);
        {
            LVFracture fracture = repository.createNewFracture(DEFAULT_TABLE);
            assertTrue (fracture.getFractureId() > 0);
            assertEquals(DEFAULT_TABLE.getTableId(), fracture.getTableId());
            
            fracture.setTupleCount(23456789L);
            fracture.setRange(new ValueRange<Integer>(300, 600));
            repository.finalizeFracture(fracture);
            fractureId2 = fracture.getFractureId();
        }
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        assertTrue(fractureId1 != fractureId2);
     
        LVFracture[] fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (3, fractures.length);
        validateFracture (fractures[1], fractureId1, 123456789L, 100, 300);
        validateFracture (fractures[2], fractureId2, 23456789L, 300, 600);

        reloadRepository();

        validateFracture (repository.getFracture(fractureId1), fractureId1, 123456789L, 100, 300);
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (3, fractures.length);
        validateFracture (fractures[1], fractureId1, 123456789L, 100, 300);
        validateFracture (fractures[2], fractureId2, 23456789L, 300, 600);
        
        repository.dropFracture(fractures[1]);

        assertNull (repository.getFracture(fractureId1));
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (2, fractures.length);
        validateFracture (fractures[1], fractureId2, 23456789L, 300, 600);

        reloadRepository();

        assertNull (repository.getFracture(fractureId1));
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (2, fractures.length);
        validateFracture (fractures[1], fractureId2, 23456789L, 300, 600);
    }
    private void validateFracture (LVFracture fracture, int fractureId, long tupleCount, int start, int end) {
        assertEquals (fractureId, fracture.getFractureId());
        assertEquals (DEFAULT_TABLE.getTableId(), fracture.getTableId());
        assertEquals (tupleCount, fracture.getTupleCount());
        assertEquals (start, fracture.getRange().getStartKey());
        assertEquals (end, fracture.getRange().getEndKey());
    }

    @Test
    public void testRackAssorted() throws IOException {
        int rackId1, rackId2;
        {
            LVRack rack = repository.createNewRack("rack1");
            assertTrue (rack.getRackId() > 0);
            validateRack (rack, rack.getRackId(), "rack1", RackStatus.OK);
            rackId1 = rack.getRackId();
        }
        {
            LVRack rack = repository.createNewRack("rack2");
            assertTrue (rack.getRackId() > 0);
            validateRack (rack, rack.getRackId(), "rack2", RackStatus.OK);
            rackId2 = rack.getRackId();
        }
        assertTrue(rackId1 != rackId2);
        try {
            // duplicated name: should fail
            repository.createNewRack("rack2");
            fail();
        } catch (IOException ex) {
            //ok.
        }

        
        LVRack[] racks = repository.getAllRacks();
        assertEquals (3, racks.length);
        validateRack (racks[0], DEFAULT_RACK.getRackId(), DEFAULT_RACK_NAME, RackStatus.OK);
        validateRack (racks[1], rackId1, "rack1", RackStatus.OK);
        validateRack (racks[2], rackId2, "rack2", RackStatus.OK);

        reloadRepository();

        validateRack (repository.getRack(rackId1), rackId1, "rack1", RackStatus.OK);
        validateRack (repository.getRack(rackId2), rackId2, "rack2", RackStatus.OK);
        racks = repository.getAllRacks();
        assertEquals (3, racks.length);
        validateRack (racks[0], DEFAULT_RACK.getRackId(), DEFAULT_RACK_NAME, RackStatus.OK);
        validateRack (racks[1], rackId1, "rack1", RackStatus.OK);
        validateRack (racks[2], rackId2, "rack2", RackStatus.OK);
        
        repository.dropRack(racks[1]);

        assertNull (repository.getRack(rackId1));
        assertNull (repository.getRack("rack1"));
        validateRack (repository.getRack(rackId2), rackId2, "rack2", RackStatus.OK);
        racks = repository.getAllRacks();
        assertEquals (2, racks.length);
        validateRack (racks[0], DEFAULT_RACK.getRackId(), DEFAULT_RACK_NAME, RackStatus.OK);
        validateRack (racks[1], rackId2, "rack2", RackStatus.OK);
        
        repository.updateRackStatus(racks[1], RackStatus.LOST);

        reloadRepository();

        assertNull (repository.getRack(rackId1));
        validateRack (repository.getRack(rackId2), rackId2, "rack2", RackStatus.LOST);
        assertEquals (2, racks.length);
        validateRack (racks[0], DEFAULT_RACK.getRackId(), DEFAULT_RACK_NAME, RackStatus.OK);
        validateRack (racks[1], rackId2, "rack2", RackStatus.LOST);
    }
    private void validateRack (LVRack rack, int rackId, String name, RackStatus status) {
        assertEquals(rackId, rack.getRackId());
        assertEquals(status, rack.getStatus());
        assertEquals(name, rack.getName());
    }

    @Test
    public void testRackNodeAssorted() throws IOException {
        int nodeId1, nodeId2;
        {
            LVRackNode node = repository.createNewRackNode(DEFAULT_RACK, "node1");
            assertTrue (node.getNodeId() > 0);
            assertEquals(DEFAULT_RACK.getRackId(), node.getRackId());
            nodeId1 = node.getNodeId();
        }
        validateRackNode (repository.getRackNode(nodeId1), nodeId1, "node1", RackNodeStatus.OK);
        validateRackNode (repository.getRackNode("node1"), nodeId1, "node1", RackNodeStatus.OK);
        {
            LVRackNode node = repository.createNewRackNode(DEFAULT_RACK, "node2");
            assertTrue (node.getNodeId() > 0);
            assertEquals(DEFAULT_RACK.getRackId(), node.getRackId());
            nodeId2 = node.getNodeId();
        }
        validateRackNode (repository.getRackNode(nodeId2), nodeId2, "node2", RackNodeStatus.OK);
        validateRackNode (repository.getRackNode("node2"), nodeId2, "node2", RackNodeStatus.OK);
        assertTrue(nodeId1 != nodeId2);
     
        LVRackNode[] nodes = repository.getAllRackNodes(DEFAULT_RACK.getRackId());
        assertEquals (3, nodes.length);
        validateRackNode (nodes[0], DEFAULT_RACK_NODE.getNodeId(), DEFAULT_RACK_NODE_NAME, RackNodeStatus.OK);
        validateRackNode (nodes[1], nodeId1, "node1", RackNodeStatus.OK);
        validateRackNode (nodes[2], nodeId2, "node2", RackNodeStatus.OK);

        reloadRepository();

        validateRackNode (repository.getRackNode(nodeId1), nodeId1, "node1", RackNodeStatus.OK);
        validateRackNode (repository.getRackNode("node1"), nodeId1, "node1", RackNodeStatus.OK);
        validateRackNode (repository.getRackNode(nodeId2), nodeId2, "node2", RackNodeStatus.OK);
        validateRackNode (repository.getRackNode("node2"), nodeId2, "node2", RackNodeStatus.OK);
        nodes = repository.getAllRackNodes(DEFAULT_RACK.getRackId());
        assertEquals (3, nodes.length);
        validateRackNode (nodes[0], DEFAULT_RACK_NODE.getNodeId(), DEFAULT_RACK_NODE_NAME, RackNodeStatus.OK);
        validateRackNode (nodes[1], nodeId1, "node1", RackNodeStatus.OK);
        validateRackNode (nodes[2], nodeId2, "node2", RackNodeStatus.OK);
        
        repository.dropRackNode(nodes[1]);

        assertNull (repository.getRackNode(nodeId1));
        assertNull (repository.getRackNode("node1"));
        validateRackNode (repository.getRackNode(nodeId2), nodeId2, "node2", RackNodeStatus.OK);
        validateRackNode (repository.getRackNode("node2"), nodeId2, "node2", RackNodeStatus.OK);
        nodes = repository.getAllRackNodes(DEFAULT_RACK.getRackId());
        assertEquals (2, nodes.length);
        validateRackNode (nodes[0], DEFAULT_RACK_NODE.getNodeId(), DEFAULT_RACK_NODE_NAME, RackNodeStatus.OK);
        validateRackNode (nodes[1], nodeId2, "node2", RackNodeStatus.OK);
        
        repository.updateRackNodeStatus(nodes[1], RackNodeStatus.LOST);

        reloadRepository();

        assertNull (repository.getRackNode(nodeId1));
        validateRackNode (repository.getRackNode(nodeId2), nodeId2, "node2", RackNodeStatus.LOST);
        validateRackNode (repository.getRackNode("node2"), nodeId2, "node2", RackNodeStatus.LOST);
        nodes = repository.getAllRackNodes(DEFAULT_RACK.getRackId());
        assertEquals (2, nodes.length);
        validateRackNode (nodes[0], DEFAULT_RACK_NODE.getNodeId(), DEFAULT_RACK_NODE_NAME, RackNodeStatus.OK);
        validateRackNode (nodes[1], nodeId2, "node2", RackNodeStatus.LOST);
    }
    private void validateRackNode (LVRackNode node, int nodeId, String name, RackNodeStatus status) {
        assertEquals (nodeId, node.getNodeId());
        assertEquals (DEFAULT_RACK.getRackId(), node.getRackId());
        assertEquals (name, node.getName());
        assertEquals (status, node.getStatus());
    }
    
    @Test
    public void testRackAssignmentAssorted() throws IOException {
        LVFracture fracture1 = DEFAULT_FRACTURE;
        LVFracture fracture2 = repository.createNewFracture(DEFAULT_TABLE);

        LVReplicaGroup group1 = DEFAULT_GROUP;
        LVReplicaGroup group2 = repository.createNewReplicaGroup(DEFAULT_TABLE, DEFAULT_COLUMNS[3]);

        LVRack rack1 = DEFAULT_RACK;
        LVRack rack2 = repository.createNewRack("rack2");

        int id11 = DEFAULT_RACK_ASSIGNMENT.getAssignmentId();
        LVRackAssignment assignment11 = repository.getRackAssignment(DEFAULT_RACK_ASSIGNMENT.getAssignmentId());
        validateRackAssignment (assignment11, id11, rack1, fracture1, group1);

        {
            LVRackAssignment[] assignments = repository.getAllRackAssignmentsByRackId(rack1.getRackId());
            assertEquals(1, assignments.length);
            validateRackAssignment (assignments[0], id11, rack1, fracture1, group1);
        }
        
        LVRackAssignment assignment12 = repository.createNewRackAssignment(rack1, fracture2, group1);
        LVRackAssignment assignment21 = repository.createNewRackAssignment(rack2, fracture1, group2);
        LVRackAssignment assignment22 = repository.createNewRackAssignment(rack2, fracture2, group2);
        int id12 = assignment12.getAssignmentId();
        int id21 = assignment21.getAssignmentId();
        int id22 = assignment22.getAssignmentId();
        validateRackAssignment (assignment12, id12, rack1, fracture2, group1);
        validateRackAssignment (assignment21, id21, rack2, fracture1, group2);
        validateRackAssignment (assignment22, id22, rack2, fracture2, group2);
        
        {
            LVRackAssignment[] assignments = repository.getAllRackAssignmentsByRackId(rack1.getRackId());
            assertEquals(2, assignments.length);
            validateRackAssignment (assignments[0], id11, rack1, fracture1, group1);
            validateRackAssignment (assignments[1], id12, rack1, fracture2, group1);
        }
        {
            LVRackAssignment[] assignments = repository.getAllRackAssignmentsByFractureId(fracture2.getFractureId());
            assertEquals(2, assignments.length);
            validateRackAssignment (assignments[0], id12, rack1, fracture2, group1);
            validateRackAssignment (assignments[1], id22, rack2, fracture2, group2);
        }
        
        repository.dropRackAssignment(assignment11);
        
        reloadRepository();

        assertNull(repository.getRackAssignment(DEFAULT_RACK_ASSIGNMENT.getAssignmentId()));
        assignment12 = repository.getRackAssignment(id12);
        assignment21 = repository.getRackAssignment(id21);
        assignment22 = repository.getRackAssignment(id22);
        validateRackAssignment (assignment12, id12, rack1, fracture2, group1);
        validateRackAssignment (assignment21, id21, rack2, fracture1, group2);
        validateRackAssignment (assignment22, id22, rack2, fracture2, group2);

        {
            LVRackAssignment[] assignments = repository.getAllRackAssignmentsByRackId(rack1.getRackId());
            assertEquals(1, assignments.length);
            validateRackAssignment (assignments[0], id12, rack1, fracture2, group1);
        }
        {
            LVRackAssignment[] assignments = repository.getAllRackAssignmentsByFractureId(fracture2.getFractureId());
            assertEquals(2, assignments.length);
            validateRackAssignment (assignments[0], id12, rack1, fracture2, group1);
            validateRackAssignment (assignments[1], id22, rack2, fracture2, group2);
        }
        
        assignment12 = repository.updateRackAssignmentOwner(assignment12, group2);
        validateRackAssignment (assignment12, id12, rack1, fracture2, group2);

        reloadRepository();
        assignment12 = repository.getRackAssignment(id12);
        validateRackAssignment (assignment12, id12, rack1, fracture2, group2);
    }
    private void validateRackAssignment(LVRackAssignment assignment, int assignmentId, LVRack rack, LVFracture fracture, LVReplicaGroup group) {
        assertEquals (assignmentId, assignment.getAssignmentId());
        assertEquals (rack.getRackId(), assignment.getRackId());
        assertEquals (fracture.getFractureId(), assignment.getFractureId());
        assertEquals (group.getGroupId(), assignment.getOwnerReplicaGroupId());
    }

    @Test
    public void testReplicaGroupAssorted() throws IOException {
        int groupId1, groupId2;
        {
            LVReplicaGroup group = repository.createNewReplicaGroup(DEFAULT_TABLE, DEFAULT_COLUMNS[3]);
            assertTrue (group.getGroupId() > 0);
            assertEquals(DEFAULT_TABLE.getTableId(), group.getTableId());
            assertEquals(DEFAULT_COLUMNS[3].getColumnId(), group.getPartitioningColumnId());
            groupId1 = group.getGroupId();
            // create another group with same partitioning
            try {
                repository.createNewReplicaGroup(DEFAULT_TABLE, DEFAULT_COLUMNS[3]);
                fail ("duplicate replica group should have been rejected... ");
            } catch (IOException ex) {
                // this IS the expected result
            }
        }
        validateGroup (repository.getReplicaGroup(groupId1), groupId1, DEFAULT_COLUMNS[3].getColumnId());
        {
            LVReplicaGroup group = repository.createNewReplicaGroup(DEFAULT_TABLE, DEFAULT_COLUMNS[2]);
            assertTrue (group.getGroupId() > 0);
            assertEquals(DEFAULT_TABLE.getTableId(), group.getTableId());
            assertEquals(DEFAULT_COLUMNS[2].getColumnId(), group.getPartitioningColumnId());
            groupId2 = group.getGroupId();
        }
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        assertTrue(groupId1 != groupId2);
     
        LVReplicaGroup[] groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (3, groups.length);
        validateGroup (groups[1], groupId1, DEFAULT_COLUMNS[3].getColumnId());
        validateGroup (groups[2], groupId2, DEFAULT_COLUMNS[2].getColumnId());

        reloadRepository();

        validateGroup (repository.getReplicaGroup(groupId1), groupId1, DEFAULT_COLUMNS[3].getColumnId());
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (3, groups.length);
        validateGroup (groups[1], groupId1, DEFAULT_COLUMNS[3].getColumnId());
        validateGroup (groups[2], groupId2, DEFAULT_COLUMNS[2].getColumnId());
        
        repository.dropReplicaGroup(groups[1]);

        assertNull (repository.getReplicaGroup(groupId1));
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (2, groups.length);
        validateGroup (groups[1], groupId2, DEFAULT_COLUMNS[2].getColumnId());

        reloadRepository();

        assertNull (repository.getReplicaGroup(groupId1));
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (2, groups.length);
        validateGroup (groups[1], groupId2, DEFAULT_COLUMNS[2].getColumnId());
    }
    private void validateGroup (LVReplicaGroup group, int groupId, int partitioningColumnId) {
        assertEquals (groupId, group.getGroupId());
        assertEquals (DEFAULT_TABLE.getTableId(), group.getTableId());
        assertEquals (partitioningColumnId, group.getPartitioningColumnId());
    }

    @Test
    public void testSubPartitionSchemeAssorted() throws IOException {
        LVFracture fracture1 = DEFAULT_FRACTURE;
        LVFracture fracture2 = repository.createNewFracture(DEFAULT_TABLE);

        LVReplicaGroup group1 = DEFAULT_GROUP;
        LVReplicaGroup group2 = repository.createNewReplicaGroup(DEFAULT_TABLE, DEFAULT_COLUMNS[2]);
        
        LVSubPartitionScheme subPartitionScheme11 = repository.getSubPartitionScheme(DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId());
        LVSubPartitionScheme subPartitionScheme12 = createSubPartitionScheme(fracture1, group2, new ValueRange<?>[]{new ValueRange<String>("A", "C"), new ValueRange<String>("C", "E")});
        LVSubPartitionScheme subPartitionScheme21 = createSubPartitionScheme(fracture2, group1, new ValueRange<?>[]{new ValueRange<Integer>(40, 150), new ValueRange<Integer>(150, 400)});
        LVSubPartitionScheme subPartitionScheme22 = createSubPartitionScheme(fracture2, group2, new ValueRange<?>[]{new ValueRange<String>("A", "BD"), new ValueRange<String>("BD", "FF")});
        
        {
            LVSubPartitionScheme[] array = repository.getAllSubPartitionSchemesByFractureId(fracture1.getFractureId());
            assertEquals(2, array.length);
            assertEquals(array[0].getSubPartitionSchemeId(), subPartitionScheme11.getSubPartitionSchemeId());
            assertEquals(array[1].getSubPartitionSchemeId(), subPartitionScheme12.getSubPartitionSchemeId());
        }
        {
            LVSubPartitionScheme[] array = repository.getAllSubPartitionSchemesByGroupId(group2.getGroupId());
            assertEquals(2, array.length);
            assertEquals(array[0].getSubPartitionSchemeId(), subPartitionScheme12.getSubPartitionSchemeId());
            assertEquals(array[1].getSubPartitionSchemeId(), subPartitionScheme22.getSubPartitionSchemeId());
        }
        {
            LVSubPartitionScheme ret = repository.getSubPartitionSchemeByFractureAndGroup(fracture2.getFractureId(), group1.getGroupId());
            assertEquals(subPartitionScheme21.getSubPartitionSchemeId(), ret.getSubPartitionSchemeId());
        }
        
        repository.dropSubPartitionScheme(subPartitionScheme12);
        reloadRepository();

        {
            LVSubPartitionScheme[] array = repository.getAllSubPartitionSchemesByFractureId(fracture1.getFractureId());
            assertEquals(1, array.length);
            assertEquals(array[0].getSubPartitionSchemeId(), subPartitionScheme11.getSubPartitionSchemeId());
        }
        {
            LVSubPartitionScheme[] array = repository.getAllSubPartitionSchemesByGroupId(group2.getGroupId());
            assertEquals(1, array.length);
            assertEquals(array[0].getSubPartitionSchemeId(), subPartitionScheme22.getSubPartitionSchemeId());
        }
        {
            LVSubPartitionScheme ret = repository.getSubPartitionSchemeByFractureAndGroup(fracture2.getFractureId(), group1.getGroupId());
            assertEquals(subPartitionScheme21.getSubPartitionSchemeId(), ret.getSubPartitionSchemeId());
        }
        assertNull(repository.getSubPartitionSchemeByFractureAndGroup(fracture1.getFractureId(), group2.getGroupId()));
    }
    private LVSubPartitionScheme createSubPartitionScheme (LVFracture fracture, LVReplicaGroup group, ValueRange<?>[] ranges) throws IOException {
        LVSubPartitionScheme subPartitionScheme = repository.createNewSubPartitionScheme(fracture, group);
        assertTrue (subPartitionScheme.getSubPartitionSchemeId() > 0);
        assertEquals(fracture.getFractureId(), subPartitionScheme.getFractureId());
        assertEquals(group.getGroupId(), subPartitionScheme.getGroupId());
        subPartitionScheme.setRanges(ranges);
        repository.finalizeSubPartitionScheme(subPartitionScheme);

        LVSubPartitionScheme forCheck = repository.getSubPartitionScheme(subPartitionScheme.getSubPartitionSchemeId());
        assertEquals(subPartitionScheme.getSubPartitionSchemeId(), forCheck.getSubPartitionSchemeId());
        assertEquals(fracture.getFractureId(), forCheck.getFractureId());
        assertEquals(group.getGroupId(), forCheck.getGroupId());
        
        assertEquals(ranges.length, forCheck.getRanges().length);
        for (int i = 0; i < ranges.length; ++i) {
            assertEquals(ranges[i], forCheck.getRanges()[i]);
        }
        
        return subPartitionScheme;
    }

    @Test
    public void testGetReplicaScheme() throws IOException {
        LVReplicaScheme scheme = repository.getReplicaScheme(DEFAULT_SCHEME.getSchemeId());
        assertEquals (DEFAULT_SCHEME.getSchemeId(), scheme.getSchemeId());
        assertEquals (DEFAULT_GROUP.getGroupId(), scheme.getGroupId());
        assertEquals (DEFAULT_COLUMNS[1].getColumnId(), scheme.getSortColumnId());

        HashMap<Integer, CompressionType> map = scheme.getColumnCompressionSchemes();
        assertEquals (CompressionType.RLE, map.get(DEFAULT_COLUMNS[0].getColumnId()));
        assertEquals (CompressionType.NULL_SUPPRESS, map.get(DEFAULT_COLUMNS[1].getColumnId()));
        assertEquals (CompressionType.DICTIONARY, map.get(DEFAULT_COLUMNS[2].getColumnId()));
        assertEquals (CompressionType.SNAPPY, map.get(DEFAULT_COLUMNS[3].getColumnId()));
        assertTrue (map.get(DEFAULT_COLUMNS[4].getColumnId()) == null
                    || map.get(DEFAULT_COLUMNS[4].getColumnId()) == CompressionType.NONE);

        assertEquals (CompressionType.RLE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[0].getColumnId()));
        assertEquals (CompressionType.NULL_SUPPRESS, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[1].getColumnId()));
        assertEquals (CompressionType.DICTIONARY, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[2].getColumnId()));
        assertEquals (CompressionType.SNAPPY, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[3].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[4].getColumnId()));
    }

    @Test
    public void testReplicaSchemeAssorted() throws IOException {
        HashMap<Integer, CompressionType> comp = new HashMap<Integer, CompressionType>();
        comp.put(DEFAULT_COLUMNS[0].getColumnId(), CompressionType.RLE);
        comp.put(DEFAULT_COLUMNS[1].getColumnId(), CompressionType.RLE);
        comp.put(DEFAULT_COLUMNS[2].getColumnId(), CompressionType.NONE);
        LVReplicaScheme scheme = repository.createNewReplicaScheme(DEFAULT_GROUP, DEFAULT_COLUMNS[2], comp);
        assertTrue (scheme.getSchemeId() > 0);
        
        assertEquals (DEFAULT_GROUP.getGroupId(), scheme.getGroupId());
        assertEquals (DEFAULT_COLUMNS[2].getColumnId(), scheme.getSortColumnId());

        assertEquals (CompressionType.RLE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[0].getColumnId()));
        assertEquals (CompressionType.RLE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[1].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[2].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[3].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[4].getColumnId()));
        
        LVReplicaScheme[] schemes = repository.getAllReplicaSchemes(DEFAULT_GROUP.getGroupId());
        assertEquals (2, schemes.length);
        assertEquals (DEFAULT_SCHEME.getSchemeId(), schemes[0].getSchemeId());
        assertEquals (scheme.getSchemeId(), schemes[1].getSchemeId());
        assertEquals (DEFAULT_COLUMNS[1].getColumnId(), schemes[0].getSortColumnId());
        assertEquals (DEFAULT_COLUMNS[2].getColumnId(), schemes[1].getSortColumnId());
        
        repository.changeColumnCompressionScheme(scheme, DEFAULT_COLUMNS[4], CompressionType.SNAPPY);
        repository.changeColumnCompressionScheme(scheme, DEFAULT_COLUMNS[1], CompressionType.NONE);

        reloadRepository();

        schemes = repository.getAllReplicaSchemes(DEFAULT_GROUP.getGroupId());
        assertEquals (2, schemes.length);
        assertEquals (DEFAULT_SCHEME.getSchemeId(), schemes[0].getSchemeId());
        assertEquals (scheme.getSchemeId(), schemes[1].getSchemeId());
        assertEquals (DEFAULT_COLUMNS[1].getColumnId(), schemes[0].getSortColumnId());
        assertEquals (DEFAULT_COLUMNS[2].getColumnId(), schemes[1].getSortColumnId());

        scheme = repository.getReplicaScheme(scheme.getSchemeId());
        assertEquals (CompressionType.RLE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[0].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[1].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[2].getColumnId()));
        assertEquals (CompressionType.NONE, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[3].getColumnId()));
        assertEquals (CompressionType.SNAPPY, scheme.getColumnCompressionScheme(DEFAULT_COLUMNS[4].getColumnId()));
        
        repository.dropReplicaScheme(schemes[0]);

        schemes = repository.getAllReplicaSchemes(DEFAULT_GROUP.getGroupId());
        assertEquals (1, schemes.length);
        assertEquals (scheme.getSchemeId(), schemes[0].getSchemeId());
        assertEquals (DEFAULT_COLUMNS[2].getColumnId(), schemes[0].getSortColumnId());
    }

    @Test
    public void testReplicaAssorted() throws IOException {
        LVFracture fracture1 = DEFAULT_FRACTURE;
        LVFracture fracture2 = repository.createNewFracture(DEFAULT_TABLE);

        LVSubPartitionScheme subPartitionScheme2 = repository.createNewSubPartitionScheme(fracture2, DEFAULT_GROUP);
        assertTrue (subPartitionScheme2.getSubPartitionSchemeId() > 0);
        assertEquals(fracture2.getFractureId(), subPartitionScheme2.getFractureId());
        assertEquals(DEFAULT_GROUP.getGroupId(), subPartitionScheme2.getGroupId());
        subPartitionScheme2.setRanges(new ValueRange<?>[]{new ValueRange<Integer>(40, 160), new ValueRange<Integer>(160, 300)});
        repository.finalizeSubPartitionScheme(subPartitionScheme2);

        LVReplicaScheme scheme1 = DEFAULT_SCHEME;
        LVReplicaScheme scheme2 = repository.createNewReplicaScheme(DEFAULT_GROUP, DEFAULT_COLUMNS[3], new HashMap<Integer, CompressionType>());

        LVReplica replica11 = repository.getReplica(DEFAULT_REPLICA.getReplicaId());
        assertEquals(DEFAULT_REPLICA.getReplicaId(), replica11.getReplicaId());

        {
            LVReplica[] replicas = repository.getAllReplicasBySchemeId(scheme1.getSchemeId());
            assertEquals(1, replicas.length);
            assertEquals(replica11.getReplicaId(), replicas[0].getReplicaId());
        }
        
        LVReplica replica12 = repository.createNewReplica(scheme1, fracture2);
        LVReplica replica21 = repository.createNewReplica(scheme2, fracture1);
        LVReplica replica22 = repository.createNewReplica(scheme2, fracture2);
        for (LVReplica replica : new LVReplica[] {replica11, replica12, replica21, replica22}) {
            assertEquals(ReplicaStatus.NOT_READY, replica.getStatus());
        }
        
        assertEquals(replica21.getReplicaId(), repository.getReplicaFromSchemeAndFracture(scheme2.getSchemeId(), fracture1.getFractureId()).getReplicaId());
        assertEquals(replica12.getReplicaId(), repository.getReplicaFromSchemeAndFracture(scheme1.getSchemeId(), fracture2.getFractureId()).getReplicaId());
        
        {
            LVReplica[] replicas = repository.getAllReplicasBySchemeId(scheme1.getSchemeId());
            assertEquals(2, replicas.length);
            assertEquals(replica11.getReplicaId(), replicas[0].getReplicaId());
            assertEquals(replica12.getReplicaId(), replicas[1].getReplicaId());
        }
        {
            LVReplica[] replicas = repository.getAllReplicasByFractureId(fracture2.getFractureId());
            assertEquals(2, replicas.length);
            assertEquals(replica12.getReplicaId(), replicas[0].getReplicaId());
            assertEquals(replica22.getReplicaId(), replicas[1].getReplicaId());
        }
        
        repository.updateReplicaStatus(replica12, ReplicaStatus.OK);
        repository.dropReplica(replica11);
        
        reloadRepository();

        assertNull(repository.getReplica(DEFAULT_REPLICA.getReplicaId()));
        replica12 = repository.getReplica(replica12.getReplicaId());
        replica21 = repository.getReplica(replica21.getReplicaId());
        replica22 = repository.getReplica(replica22.getReplicaId());
        assertEquals(ReplicaStatus.OK, replica12.getStatus());
        assertEquals(ReplicaStatus.NOT_READY, replica21.getStatus());
        assertEquals(ReplicaStatus.NOT_READY, replica22.getStatus());

        {
            LVReplica[] replicas = repository.getAllReplicasBySchemeId(scheme1.getSchemeId());
            assertEquals(1, replicas.length);
            assertEquals(replica12.getReplicaId(), replicas[0].getReplicaId());
        }
        {
            LVReplica[] replicas = repository.getAllReplicasByFractureId(fracture2.getFractureId());
            assertEquals(2, replicas.length);
            assertEquals(replica12.getReplicaId(), replicas[0].getReplicaId());
            assertEquals(replica22.getReplicaId(), replicas[1].getReplicaId());
        }
    }
    
    @Test
    public void testGetReplicaPartition() throws IOException {
        LVReplicaPartition partition = repository.getReplicaPartition(DEFAULT_REPLICA_PARTITIONS[1].getPartitionId());
        assertEquals(DEFAULT_REPLICA.getReplicaId(), partition.getReplicaId());
        assertEquals(1, partition.getRange());
        assertEquals(DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId(), partition.getSubPartitionSchemeId());
        assertEquals(ReplicaPartitionStatus.OK, partition.getStatus());
    }

    @Test
    public void testGetAllReplicaPartitionsByReplicaId() throws IOException {
        LVReplicaPartition[] partitions = repository.getAllReplicaPartitionsByReplicaId(DEFAULT_REPLICA.getReplicaId());
        assertEquals(2, partitions.length);
        assertEquals(DEFAULT_REPLICA_PARTITIONS[0].getPartitionId(), partitions[0].getPartitionId());
        assertEquals(DEFAULT_REPLICA_PARTITIONS[1].getPartitionId(), partitions[1].getPartitionId());
    }

    @Test
    public void testGetReplicaPartitionByReplicaAndRange() throws IOException {
        LVReplicaPartition partition = repository.getReplicaPartitionByReplicaAndRange(DEFAULT_REPLICA.getReplicaId(), 1);
        assertEquals(DEFAULT_REPLICA.getReplicaId(), partition.getReplicaId());
        assertEquals(1, partition.getRange());
        assertEquals(DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId(), partition.getSubPartitionSchemeId());
        assertEquals(ReplicaPartitionStatus.OK, partition.getStatus());
        assertNull(repository.getReplicaPartitionByReplicaAndRange(DEFAULT_REPLICA.getReplicaId(), 2));
    }

    @Test
    public void testReplicaPartitionAssorted() throws IOException {
        LVReplicaScheme scheme = repository.createNewReplicaScheme(DEFAULT_GROUP, DEFAULT_COLUMNS[3], new HashMap<Integer, CompressionType>());
        LVReplica replica = repository.createNewReplica(scheme, DEFAULT_FRACTURE);
        
        assertTrue (replica.getReplicaId() > 0);
        assertEquals(scheme.getSchemeId(), replica.getSchemeId());
        assertEquals(DEFAULT_FRACTURE.getFractureId(), replica.getFractureId());
        assertEquals(DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId(), replica.getSubPartitionSchemeId());
        
        LVReplicaPartition[] partitions = new LVReplicaPartition[2];
        for (int i = 0; i < 2; ++i) {
            partitions[i] = repository.createNewReplicaPartition(replica, i);
            assertEquals(replica.getReplicaId(), partitions[i].getReplicaId());
            assertEquals(i, partitions[i].getRange());
            assertEquals(DEFAULT_SUB_PARTITION_SCHEME.getSubPartitionSchemeId(), partitions[i].getSubPartitionSchemeId());
            assertEquals(ReplicaPartitionStatus.BEING_RECOVERED, partitions[i].getStatus());
            partitions[i] = repository.updateReplicaPartition(partitions[i], ReplicaPartitionStatus.OK, DEFAULT_RACK_NODE);
            assertEquals(ReplicaPartitionStatus.OK, partitions[i].getStatus());
        }
        {
            LVReplicaPartition[] ret = repository.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assertEquals(2, ret.length);
            assertEquals(partitions[0].getPartitionId(), ret[0].getPartitionId());
            assertEquals(partitions[1].getPartitionId(), ret[1].getPartitionId());
        }

        partitions[0] = repository.updateReplicaPartition(partitions[0], ReplicaPartitionStatus.LOST, DEFAULT_RACK_NODE);
        assertEquals(ReplicaPartitionStatus.LOST, partitions[0].getStatus());
        assertEquals(DEFAULT_RACK_NODE.getNodeId(), partitions[0].getNodeId().intValue());
        
        repository.dropReplicaPartition(partitions[0]);

        reloadRepository();
        
        {
            LVReplicaPartition[] ret = repository.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assertEquals(1, ret.length);
            assertEquals(partitions[1].getPartitionId(), ret[0].getPartitionId());
        }
        assertNull(repository.getReplicaPartition(partitions[0].getPartitionId()));
        {
            LVReplicaPartition ret = repository.getReplicaPartitionByReplicaAndRange(replica.getReplicaId(), 1);
            assertEquals(partitions[1].getPartitionId(), ret.getPartitionId());
        }
    }

    @Test
    public void testGetColumnFile() throws IOException {
        validateColumnFile (repository.getColumnFile(DEFAULT_COLUMN_FILES[0][3].getColumnFileId()), 0, 3);
        validateColumnFile (repository.getColumnFileByReplicaPartitionAndColumn(DEFAULT_REPLICA_PARTITIONS[1].getPartitionId(), DEFAULT_COLUMNS[0].getColumnId()), 1, 0);
    }
    private void validateColumnFile(LVColumnFile file, int partition, int col) {
        assertEquals(DEFAULT_COLUMNS[col].getColumnId(), file.getColumnId());
        assertEquals(DEFAULT_COLUMN_FILES[partition][col].getFileSize(), file.getFileSize());
        assertEquals(DEFAULT_COLUMN_FILES[partition][col].getChecksum(), file.getChecksum());
        assertEquals(DEFAULT_REPLICA_PARTITIONS[partition].getPartitionId(), file.getPartitionId());
        assertEquals("hdfs://dummy_url_" + partition + "/colfile" + col, file.getHdfsFilePath());
    }

    @Test
    public void testGetAllColumnFilesByReplicaPartitionId() throws IOException {
        for (int partition = 0; partition < 2; ++partition) {
            LVColumnFile[] files = repository.getAllColumnFilesByReplicaPartitionId(DEFAULT_REPLICA_PARTITIONS[partition].getPartitionId());
            for (int col = 0; col < DEFAULT_COLUMNS.length; ++col) {
                validateColumnFile (files[col], partition, col);
            }
        }
    }

    @Test
    public void testColumnFileAssorted() throws IOException {
        LVColumn column = repository.createNewColumn(DEFAULT_TABLE, "newcol", ColumnType.BIGINT);
        validateNewColumn(column);
        
        LVColumnFile[] files = new LVColumnFile[2];
        for (int i = 0; i < 2; ++i) {
            files[i] = repository.createNewColumnFile(DEFAULT_REPLICA_PARTITIONS[i],
                            column, "hdfs://dummy_url_" + i + "/newcolfile" + i, 5763241L, 12176);
            assertEquals(column.getColumnId(), files[i].getColumnId());
            assertEquals(5763241L, files[i].getFileSize());
            assertEquals(12176, files[i].getChecksum());
            assertEquals(DEFAULT_REPLICA_PARTITIONS[i].getPartitionId(), files[i].getPartitionId());
        }
        
        repository.dropColumnFile(files[1]);
        reloadRepository();
        
        {
            LVColumnFile[] ret = repository.getAllColumnFilesByReplicaPartitionId(DEFAULT_REPLICA_PARTITIONS[0].getPartitionId());
            assertEquals(DEFAULT_COLUMNS.length + 1, ret.length);
            assertEquals(files[0].getColumnFileId(), ret[DEFAULT_COLUMNS.length].getColumnFileId());
            assertEquals(column.getColumnId(), ret[DEFAULT_COLUMNS.length].getColumnId());
        }
        
        {
            LVColumnFile[] ret = repository.getAllColumnFilesByReplicaPartitionId(DEFAULT_REPLICA_PARTITIONS[1].getPartitionId());
            assertEquals(DEFAULT_COLUMNS.length, ret.length);
            for (LVColumnFile file : ret) {
                assertTrue (file.getColumnFileId() != files[1].getColumnFileId());
                assertTrue (file.getColumnId() != column.getColumnId());
            }
        }
    }
}
