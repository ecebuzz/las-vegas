package edu.brown.lasvegas;

import static org.junit.Assert.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcases for serialization/deserialization of LVObject classes. 
 */
public class LVObjectSerializationTest {
    private DataOutputStream out;
    private ByteArrayOutputStream outBytes;
    private byte[] inBytes;

    private void initOut () throws IOException {
        outBytes = new ByteArrayOutputStream();
        out = new DataOutputStream(outBytes);
    }
    private DataInputStream inFromOut () throws IOException {
        inBytes = outBytes.toByteArray();
        return new DataInputStream(new ByteArrayInputStream(inBytes));
    }
    
    @Before
    public void setUp () throws IOException {
        initOut();
    }

    @After
    public void tearDown () throws IOException {
        if (out != null) {
            out.close();
            out = null;
            outBytes = null;
        }
    }

    @Test
    public void testColumn () throws IOException {
        LVColumn obj = new LVColumn();
        obj.setColumnId(32);
        obj.setFracturingColumn(false);
        obj.setName("dlfg");
        obj.setOrder(3);
        obj.setStatus(ColumnStatus.OK);
        obj.setTableId(3333);
        obj.setType(ColumnType.FLOAT);
        obj.write(out);
        LVColumn obj2 = LVColumn.read(inFromOut());
        assertEquals(32, obj2.getColumnId());
        assertEquals(false, obj2.isFracturingColumn());
        assertEquals("dlfg", obj2.getName());
        assertEquals(3, obj2.getOrder());
        assertEquals(ColumnStatus.OK, obj2.getStatus());
        assertEquals(3333, obj2.getTableId());
        assertEquals(ColumnType.FLOAT, obj2.getType());
        obj2.setFracturingColumn(true);
        obj2.setColumnId(33);
        obj2.write(out);

        DataInputStream in = inFromOut();
        LVColumn obj1 = LVColumn.read(in);
        obj2 = LVColumn.read(in);

        assertEquals(32, obj1.getColumnId());
        assertEquals(33, obj2.getColumnId());
        assertEquals(false, obj1.isFracturingColumn());
        assertEquals(true, obj2.isFracturingColumn());
    }

    @Test
    public void testColumnFile () throws IOException {
        LVColumnFile obj1 = new LVColumnFile();
        obj1.setColumnFileId(32);
        obj1.setChecksum(123456);
        obj1.setColumnId(88);
        obj1.setFileSize(6666);
        obj1.setLocalFilePath(null);
        obj1.setAverageRunLength(333);
        obj1.setDictionaryBytesPerEntry((byte)4);
        obj1.setDistinctValues(35545);
        obj1.setPartitionId(5544);
        obj1.write(out);
        LVColumnFile obj2 = new LVColumnFile();
        obj2.setColumnFileId(12);
        obj2.setChecksum(1234567);
        obj2.setColumnId(885);
        obj2.setDistinctValues(24545);
        obj2.setFileSize(66665);
        obj2.setLocalFilePath("dfgkj");
        obj2.setPartitionId(88863);
        obj2.write(out);

        LVColumnFile[] org = new LVColumnFile[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVColumnFile copied = LVColumnFile.read(in);
            assertEquals(org[i].getAverageRunLength(), copied.getAverageRunLength());
            assertEquals(org[i].getChecksum(), copied.getChecksum());
            assertEquals(org[i].getColumnFileId(), copied.getColumnFileId());
            assertEquals(org[i].getColumnId(), copied.getColumnId());
            assertEquals(org[i].getDictionaryBytesPerEntry(), copied.getDictionaryBytesPerEntry());
            assertEquals(org[i].getDistinctValues(), copied.getDistinctValues());
            assertEquals(org[i].getFileSize(), copied.getFileSize());
            assertEquals(org[i].getLocalFilePath(), copied.getLocalFilePath());
            assertEquals(org[i].getPartitionId(), copied.getPartitionId());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testDatabase () throws IOException {
        LVDatabase obj1 = new LVDatabase();
        obj1.setDatabaseId(32);
        obj1.setName("db1");
        obj1.setStatus(DatabaseStatus.BEING_DROPPED);
        obj1.write(out);
        LVDatabase obj2 = new LVDatabase();
        obj2.setDatabaseId(442);
        obj2.setName("rack2");
        obj2.setStatus(DatabaseStatus.OK);
        obj2.write(out);

        LVDatabase[] org = new LVDatabase[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVDatabase copied = LVDatabase.read(in);
            assertEquals(org[i].getDatabaseId(), copied.getDatabaseId());
            assertEquals(org[i].getName(), copied.getName());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testFracture () throws IOException {
        LVFracture obj1 = new LVFracture();
        obj1.setFractureId(32);
        obj1.setRange(new ValueRange(ColumnType.BIGINT, -902834L, 34342L));
        obj1.setTableId(6666);
        obj1.setTupleCount(243643545L);
        obj1.write(out);
        LVFracture obj2 = new LVFracture();
        obj2.setFractureId(543);
        obj2.setRange(new ValueRange(ColumnType.FLOAT, null, 9823.348f));
        obj2.setTableId(454);
        obj2.setTupleCount(0L);
        obj2.write(out);

        LVFracture[] org = new LVFracture[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVFracture copied = LVFracture.read(in);
            assertEquals(org[i].getFractureId(), copied.getFractureId());
            assertEquals(org[i].getRange(), copied.getRange());
            assertEquals(org[i].getTableId(), copied.getTableId());
            assertEquals(org[i].getTupleCount(), copied.getTupleCount());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testRack () throws IOException {
        LVRack obj1 = new LVRack();
        obj1.setRackId(32);
        obj1.setName("rack1");
        obj1.setStatus(RackStatus.LOST);
        obj1.write(out);
        LVRack obj2 = new LVRack();
        obj2.setRackId(442);
        obj2.setName("rack2");
        obj2.setStatus(RackStatus.OK);
        obj2.write(out);

        LVRack[] org = new LVRack[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVRack copied = LVRack.read(in);
            assertEquals(org[i].getRackId(), copied.getRackId());
            assertEquals(org[i].getName(), copied.getName());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testRackAssignment () throws IOException {
        LVRackAssignment obj1 = new LVRackAssignment();
        obj1.setAssignmentId(32);
        obj1.setFractureId(5465465);
        obj1.setOwnerReplicaGroupId(6444);
        obj1.setRackId(4477788);
        obj1.write(out);
        LVRackAssignment obj2 = new LVRackAssignment();
        obj2.setAssignmentId(3311);
        obj2.setFractureId(4989942);
        obj2.setOwnerReplicaGroupId(200);
        obj2.setRackId(5);
        obj2.write(out);

        LVRackAssignment[] org = new LVRackAssignment[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVRackAssignment copied = LVRackAssignment.read(in);
            assertEquals(org[i].getAssignmentId(), copied.getAssignmentId());
            assertEquals(org[i].getFractureId(), copied.getFractureId());
            assertEquals(org[i].getOwnerReplicaGroupId(), copied.getOwnerReplicaGroupId());
            assertEquals(org[i].getRackId(), copied.getRackId());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testRackNode () throws IOException {
        LVRackNode obj1 = new LVRackNode();
        obj1.setNodeId(32);
        obj1.setName("node1");
        obj1.setStatus(RackNodeStatus.LOST);
        obj1.setRackId(4477788);
        obj1.write(out);
        LVRackNode obj2 = new LVRackNode();
        obj2.setNodeId(3311);
        obj2.setName("node2");
        obj2.setStatus(RackNodeStatus.OK);
        obj2.setRackId(5);
        obj2.write(out);

        LVRackNode[] org = new LVRackNode[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVRackNode copied = LVRackNode.read(in);
            assertEquals(org[i].getNodeId(), copied.getNodeId());
            assertEquals(org[i].getName(), copied.getName());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getRackId(), copied.getRackId());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testReplica () throws IOException {
        LVReplica obj1 = new LVReplica();
        obj1.setFractureId(32);
        obj1.setReplicaId(545454);
        obj1.setSchemeId(8730);
        obj1.setStatus(ReplicaStatus.NOT_READY);
        obj1.write(out);
        LVReplica obj2 = new LVReplica();
        obj2.setFractureId(10);
        obj2.setReplicaId(1);
        obj2.setSchemeId(5536);
        obj2.setStatus(ReplicaStatus.OK);
        obj2.write(out);

        LVReplica[] org = new LVReplica[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVReplica copied = LVReplica.read(in);
            assertEquals(org[i].getFractureId(), copied.getFractureId());
            assertEquals(org[i].getReplicaId(), copied.getReplicaId());
            assertEquals(org[i].getSchemeId(), copied.getSchemeId());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testReplicaGroup () throws IOException {
        LVReplicaGroup obj1 = new LVReplicaGroup();
        obj1.setGroupId(32);
        obj1.setPartitioningColumnId(21);
        obj1.setTableId(54);
        obj1.setRanges(new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, 20), new ValueRange(ColumnType.INTEGER, 20, 50), new ValueRange(ColumnType.INTEGER, 50, null)});
        obj1.write(out);
        LVReplicaGroup obj2 = new LVReplicaGroup();
        obj2.setGroupId(42);
        obj2.setPartitioningColumnId(210);
        obj2.setTableId(354);
        obj2.write(out);

        LVReplicaGroup[] org = new LVReplicaGroup[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVReplicaGroup copied = LVReplicaGroup.read(in);
            assertEquals(org[i].getGroupId(), copied.getGroupId());
            assertEquals(org[i].getPartitioningColumnId(), copied.getPartitioningColumnId());
            assertEquals(org[i].getTableId(), copied.getTableId());
            assertArrayEquals(org[i].getRanges(), copied.getRanges());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }


    @Test
    public void testReplicaPartition () throws IOException {
        LVReplicaPartition obj1 = new LVReplicaPartition();
        obj1.setNodeId(1234);
        obj1.setPartitionId(32);
        obj1.setRange(0);
        obj1.setReplicaId(555543);
        obj1.setReplicaGroupId(345);
        obj1.setStatus(ReplicaPartitionStatus.BEING_RECOVERED);
        obj1.write(out);
        LVReplicaPartition obj2 = new LVReplicaPartition();
        obj2.setNodeId(null);
        obj2.setPartitionId(333);
        obj2.setRange(11);
        obj2.setReplicaId(342312);
        obj2.setReplicaGroupId(2);
        obj2.setStatus(ReplicaPartitionStatus.LOST);
        obj2.write(out);

        LVReplicaPartition[] org = new LVReplicaPartition[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVReplicaPartition copied = LVReplicaPartition.read(in);
            assertEquals(org[i].getNodeId(), copied.getNodeId());
            assertEquals(org[i].getPartitionId(), copied.getPartitionId());
            assertEquals(org[i].getRange(), copied.getRange());
            assertEquals(org[i].getReplicaId(), copied.getReplicaId());
            assertEquals(org[i].getReplicaRange(), copied.getReplicaRange());
            assertEquals(org[i].getReplicaGroupId(), copied.getReplicaGroupId());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testReplicaScheme () throws IOException {
        LVReplicaScheme obj1 = new LVReplicaScheme();
        obj1.setGroupId(433);
        obj1.setSchemeId(32);
        obj1.setSortColumnId(32);
        HashMap<Integer, CompressionType> types1 = new HashMap<Integer, CompressionType>();
        types1.put(3, CompressionType.DICTIONARY);
        types1.put(1, CompressionType.NONE);
        types1.put(8, CompressionType.RLE);
        obj1.setColumnCompressionSchemes(types1);
        obj1.write(out);
        LVReplicaScheme obj2 = new LVReplicaScheme();
        obj2.setGroupId(45);
        obj2.setSchemeId(222);
        obj2.setSortColumnId(2);
        HashMap<Integer, CompressionType> types2 = new HashMap<Integer, CompressionType>();
        types2.put(1, CompressionType.NONE);
        types2.put(2, CompressionType.GZIP_BEST_COMPRESSION);
        types2.put(8, CompressionType.NONE);
        obj2.setColumnCompressionSchemes(types2);
        obj2.write(out);

        LVReplicaScheme[] org = new LVReplicaScheme[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVReplicaScheme copied = LVReplicaScheme.read(in);
            assertEquals(org[i].getGroupId(), copied.getGroupId());
            assertEquals(org[i].getSchemeId(), copied.getSchemeId());
            assertEquals(org[i].getSortColumnId(), copied.getSortColumnId());
            assertEquals(org[i].getColumnCompressionSchemes(), copied.getColumnCompressionSchemes());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testTable () throws IOException {
        LVTable obj1 = new LVTable();
        obj1.setTableId(32);
        obj1.setFracturingColumnId(232);
        obj1.setPervasiveReplication(true);
        obj1.setStatus(TableStatus.BEING_CREATED);
        obj1.setName("sdg");
        obj1.write(out);
        LVTable obj2 = new LVTable();
        obj2.setTableId(1);
        obj2.setFracturingColumnId(33);
        obj2.setPervasiveReplication(false);
        obj2.setStatus(TableStatus.OK);
        obj2.setName("hgfg2");
        obj2.write(out);

        LVTable[] org = new LVTable[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVTable copied = LVTable.read(in);
            assertEquals(org[i].getTableId(), copied.getTableId());
            assertEquals(org[i].getFracturingColumnId(), copied.getFracturingColumnId());
            assertEquals(org[i].isPervasiveReplication(), copied.isPervasiveReplication());
            assertEquals(org[i].getTableId(), copied.getTableId());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getName(), copied.getName());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testJob () throws IOException {
        LVJob obj1 = new LVJob();
        obj1.setDescription("job1");
        obj1.setErrorMessages("user cancel");
        obj1.setFinishedTime(new Date(201010101010L));
        obj1.setJobId(32);
        obj1.setProgress(0.3d);
        obj1.setStartedTime(new Date(101010101010L));
        obj1.setStatus(JobStatus.CANCELED);
        obj1.setType(JobType.MERGE_FRACTURE);
        obj1.write(out);
        LVJob obj2 = new LVJob();
        obj2.setJobId(442);
        obj2.setDescription("job2");
        obj2.setStatus(JobStatus.ERROR);
        obj2.setParameters(new byte[]{(byte)23, (byte)-23, (byte)0, (byte)32});
        obj2.write(out);

        LVJob[] org = new LVJob[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVJob copied = LVJob.read(in);
            assertEquals(org[i].getJobId(), copied.getJobId());
            assertEquals(org[i].getDescription(), copied.getDescription());
            assertEquals(org[i].getErrorMessages(), copied.getErrorMessages());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getType(), copied.getType());
            assertEquals(org[i].getProgress(), copied.getProgress(), 0.00000001d);
            assertEquals(org[i].getStartedTime(), copied.getStartedTime());
            assertEquals(org[i].getFinishedTime(), copied.getFinishedTime());
            assertArrayEquals(org[i].getParameters(), copied.getParameters());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }

    @Test
    public void testTask () throws IOException {
        LVTask obj1 = new LVTask();
        obj1.setErrorMessages("liilisdsd");
        obj1.setFinishedTime(new Date(201010101010L));
        obj1.setJobId(32);
        obj1.setProgress(0.3d);
        obj1.setStartedTime(new Date(101010101010L));
        obj1.setTaskId(32);
        obj1.setNodeId(123);
        obj1.setStatus(TaskStatus.CREATED);
        obj1.setType(TaskType.PROJECT);
        obj1.setOutputFilePaths(new String[]{"aaa", "bbb"});
        obj1.setParameters(new byte[]{(byte)23, (byte)-23, (byte)0, (byte)32});
        obj1.write(out);
        LVTask obj2 = new LVTask();
        obj2.setErrorMessages("sdfsdfsdf");
        obj2.setFinishedTime(new Date(21010101010L));
        obj2.setJobId(454);
        obj2.setProgress(0.3d);
        obj2.setStartedTime(new Date(11010101010L));
        obj2.setTaskId(3333);
        obj2.setNodeId(1323232);
        obj2.setStatus(TaskStatus.RUNNING);
        obj2.setType(TaskType.FILTER_COLUMN_FILES);
        obj2.setOutputFilePaths(new String[]{"asd"});
        obj2.write(out);

        LVTask[] org = new LVTask[]{obj1, obj2};
        DataInputStream in = inFromOut();
        for (int i = 0; i < org.length; ++i) {
            LVTask copied = LVTask.read(in);
            assertEquals(org[i].getTaskId(), copied.getTaskId());
            assertEquals(org[i].getErrorMessages(), copied.getErrorMessages());
            assertEquals(org[i].getJobId(), copied.getJobId());
            assertEquals(org[i].getFinishedTime(), copied.getFinishedTime());
            assertEquals(org[i].getProgress(), copied.getProgress(), 0.0000001d);
            assertEquals(org[i].getStartedTime(), copied.getStartedTime());
            assertEquals(org[i].getNodeId(), copied.getNodeId());
            assertEquals(org[i].getStatus(), copied.getStatus());
            assertEquals(org[i].getType(), copied.getType());
            assertArrayEquals(org[i].getOutputFilePaths(), copied.getOutputFilePaths());
            assertArrayEquals(org[i].getParameters(), copied.getParameters());
            assertEquals(org[i].getPrimaryKey(), copied.getPrimaryKey());
        }
    }
}
