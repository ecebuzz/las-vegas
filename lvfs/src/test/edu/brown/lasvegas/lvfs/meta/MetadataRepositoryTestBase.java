package edu.brown.lasvegas.lvfs.meta;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVTable;

/**
 * Base class of testcases for Metadata repository.
 * As each implementation should behave in a same way,
 * most of tests are defined here regardless the test is for
 * master or slave.
 * 
 * Name of this abstract class doesn't end with Test so that our ant script
 * easily skip this. 
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
    public void testGetTable() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewTable() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testRequestDropTable() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropTable() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllColumns() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetColumn() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewColumn() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testRequestDropColumn() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropColumn() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetFracture() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllFractures() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewFracture() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testFinalizeFracture() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropFracture() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetReplicaGroup() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGetAllReplicaGroups() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateNewReplicaGroup() throws IOException {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDropReplicaGroup() throws IOException {
        fail("Not yet implemented"); // TODO
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
