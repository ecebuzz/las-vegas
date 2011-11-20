package edu.brown.lasvegas.lvfs.meta;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import edu.brown.lasvegas.ColumnStatus;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TableStatus;
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
    protected MetadataRepository repository;
    
    /**
     * Flushes the tested repository, closes it and then reloads it.
     * Used to test durability of the repository.
     * @throws IOException
     */
    protected abstract void reloadRepository() throws IOException;

    /** called from setUp() in derived class. */
    protected void baseSetUp(MetadataRepository repository) throws Exception {
        this.repository = repository;
        initDefaultTestObjects ();
    }

    /** called from tearDown() in derived class. */
    protected void baseTearDown() throws Exception {
        repository = null;
    }
    private LVTable DEFAULT_TABLE;
    private final static String DEFAULT_TABLE_NAME = "deftable";
    private LVColumn[] DEFAULT_COLUMNS;
    
    /** for ease of testing, create a few default objects. */
    private void initDefaultTestObjects () throws IOException {
        LVTable existing = repository.getTable(DEFAULT_TABLE_NAME);
        if (existing != null) {
            repository.dropTable(existing);
        }
        DEFAULT_TABLE = repository.createNewTable(DEFAULT_TABLE_NAME, new LVColumn[]{
            new LVColumn("intcol", ColumnType.INTEGER),
            new LVColumn("strcol", ColumnType.VARCHAR),
            new LVColumn("floatcol", ColumnType.FLOAT),
            new LVColumn("tscol", ColumnType.TIMESTAMP),
        });
        assertTrue (DEFAULT_TABLE.getTableId() != 0);
        
        DEFAULT_COLUMNS = repository.getAllColumns(DEFAULT_TABLE.getTableId());
        assertEquals (4 + 1, DEFAULT_COLUMNS.length);
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
        int table1 = repository.issueNewId(LVTable.class);
        int fracture1 = repository.issueNewId(LVFracture.class);
        int table2 = repository.issueNewId(LVTable.class);
        int fracture2 = repository.issueNewId(LVFracture.class);
        int table3 = repository.issueNewId(LVTable.class);
        assertEquals (table1 + 1, table2);
        assertEquals (table1 + 2, table3);
        assertEquals (fracture1 + 1, fracture2);

        reloadRepository();

        int fracture3 = repository.issueNewId(LVFracture.class);
        int fracture4 = repository.issueNewId(LVFracture.class);
        int table4 = repository.issueNewId(LVTable.class);

        assertEquals (table1 + 3, table4);
        assertEquals (fracture1 + 2, fracture3);
        assertEquals (fracture1 + 3, fracture4);
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
        assertEquals (2, fractures.length);
        validateFracture (fractures[0], fractureId1, 123456789L, 100, 300);
        validateFracture (fractures[1], fractureId2, 23456789L, 300, 600);

        reloadRepository();

        validateFracture (repository.getFracture(fractureId1), fractureId1, 123456789L, 100, 300);
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (2, fractures.length);
        validateFracture (fractures[0], fractureId1, 123456789L, 100, 300);
        validateFracture (fractures[1], fractureId2, 23456789L, 300, 600);
        
        repository.dropFracture(fractures[0]);

        assertNull (repository.getFracture(fractureId1));
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (1, fractures.length);
        validateFracture (fractures[0], fractureId2, 23456789L, 300, 600);

        reloadRepository();

        assertNull (repository.getFracture(fractureId1));
        validateFracture (repository.getFracture(fractureId2), fractureId2, 23456789L, 300, 600);
        fractures = repository.getAllFractures(DEFAULT_TABLE.getTableId());
        assertEquals (1, fractures.length);
        validateFracture (fractures[0], fractureId2, 23456789L, 300, 600);
    }
    private void validateFracture (LVFracture fracture, int fractureId, long tupleCount, int start, int end) {
        assertEquals (fractureId, fracture.getFractureId());
        assertEquals (DEFAULT_TABLE.getTableId(), fracture.getTableId());
        assertEquals (tupleCount, fracture.getTupleCount());
        assertEquals (start, fracture.getRange().getStartKey());
        assertEquals (end, fracture.getRange().getEndKey());
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
        assertEquals (2, groups.length);
        validateGroup (groups[0], groupId1, DEFAULT_COLUMNS[3].getColumnId());
        validateGroup (groups[1], groupId2, DEFAULT_COLUMNS[2].getColumnId());

        reloadRepository();

        validateGroup (repository.getReplicaGroup(groupId1), groupId1, DEFAULT_COLUMNS[3].getColumnId());
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (2, groups.length);
        validateGroup (groups[0], groupId1, DEFAULT_COLUMNS[3].getColumnId());
        validateGroup (groups[1], groupId2, DEFAULT_COLUMNS[2].getColumnId());
        
        repository.dropReplicaGroup(groups[0]);

        assertNull (repository.getReplicaGroup(groupId1));
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (1, groups.length);
        validateGroup (groups[0], groupId2, DEFAULT_COLUMNS[2].getColumnId());

        reloadRepository();

        assertNull (repository.getReplicaGroup(groupId1));
        validateGroup (repository.getReplicaGroup(groupId2), groupId2, DEFAULT_COLUMNS[2].getColumnId());
        groups = repository.getAllReplicaGroups(DEFAULT_TABLE.getTableId());
        assertEquals (1, groups.length);
        validateGroup (groups[0], groupId2, DEFAULT_COLUMNS[2].getColumnId());
    }
    private void validateGroup (LVReplicaGroup group, int groupId, int partitioningColumnId) {
        assertEquals (groupId, group.getGroupId());
        assertEquals (DEFAULT_TABLE.getTableId(), group.getTableId());
        assertEquals (partitioningColumnId, group.getPartitioningColumnId());
    }

    @Test
    public void testGetReplicaScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllReplicaSchemes() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewReplicaScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testChangeColumnCompressionScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropReplicaScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetReplica() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllReplicasBySchemeId() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllReplicasByFractureId() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetReplicaFromSchemeAndFracture() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewReplica() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testUpdateReplicaStatus() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropReplica() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetSubPartitionScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllSubPartitionSchemesByFractureId() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllSubPartitionSchemesByGroupId() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetSubPartitionSchemeByFractureAndGroup() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewSubPartitionScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testFinalizeSubPartitionScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropSubPartitionScheme() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetReplicaPartition() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllReplicaPartitionsByReplicaId() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetReplicaPartitionByReplicaAndRange() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewReplicaPartition() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testUpdateReplicaPartition() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropReplicaPartition() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetColumnFile() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllColumnFilesByReplicaPartitionId() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetColumnFileByReplicaPartitionAndColumn() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewColumnFile() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropColumnFile() throws IOException {
        fail("Not yet implemented"); // TODO
    }
}
