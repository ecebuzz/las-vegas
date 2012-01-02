package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.TypedDictReader;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.tuple.BufferedTupleWriter;
import edu.brown.lasvegas.tuple.TextFileTupleReader;

/** testcase for {@link PartitionRewriter}. */ 
public class PartitionRewriterTest {
    private VirtualFile tmpFolder;
    private ColumnFileBundle[] columnFiles;
    private CompressionType[] compressions;
    @Before
    public void setUp () throws Exception {
        tmpFolder = new LocalVirtualFile("test/imp/");
        if (tmpFolder.exists()) {
            tmpFolder.delete(true);
        }
        tmpFolder.mkdirs();
        if (!tmpFolder.exists()) {
            throw new IOException ("can't make a temporary folder: " + tmpFolder);
        }

        TextFileTupleReader reader = MiniLineorder.open();
        compressions = MiniLineorder.getDefaultCompressions();
        String[] names = new String[compressions.length];
        for (int i = 0; i < names.length; ++i) names[i] = "tmp_" + i;
        BufferedTupleWriter writer = new BufferedTupleWriter(reader, 1 << 10, tmpFolder, compressions, names, false);
        int appended = writer.appendAllTuples();
        assertEquals (45, appended);
        columnFiles = writer.finish();
        writer.close();
        reader.close();
    }
    
    @Test
    public void testPrioritySort () throws Exception {
        String[] newNames = new String[compressions.length];
        for (int i = 0; i < newNames.length; ++i) newNames[i] = "sorted_" + i;
        PartitionRewriter rewriter = new PartitionRewriter(tmpFolder, columnFiles, newNames, compressions, 6); // by lo_orderpriority
        ColumnFileBundle[] sortedFiles = rewriter.execute();
        
        ColumnFileReaderBundle file = new ColumnFileReaderBundle(sortedFiles[6]);
        @SuppressWarnings("unchecked")
        TypedDictReader<String, String[], Byte, byte[]> reader = (TypedDictReader<String, String[], Byte, byte[]>) file.getDataReader();
        assertEquals (45, reader.getTotalTuples());
        assertEquals ("1-URGENT", reader.readValue());
        assertEquals ("1-URGENT", reader.readValue());
        String[] remaining = new String[45 - 2];
        int read = reader.readValues(remaining, 0, remaining.length);
        assertEquals (remaining.length, read);
        assertArrayEquals(new String[]{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECI", "5-LOW"}, reader.getDict().getDictionary());
        file.close();
    }

    @Test
    public void testOrderdateSort () throws Exception {
        String[] newNames = new String[compressions.length];
        for (int i = 0; i < newNames.length; ++i) newNames[i] = "sorted_" + i;
        PartitionRewriter rewriter = new PartitionRewriter(tmpFolder, columnFiles, newNames, compressions, 5); // by lo_orderdate
        ColumnFileBundle[] sortedFiles = rewriter.execute();
        
        ColumnFileReaderBundle file = new ColumnFileReaderBundle(sortedFiles[5]);
        @SuppressWarnings("unchecked")
        TypedReader<Integer, int[]> reader = (TypedReader<Integer, int[]>) file.getDataReader();
        assertEquals (45, reader.getTotalTuples());
        assertEquals (19930111, reader.readValue().intValue());
        assertEquals (19930111, reader.readValue().intValue());
        int[] remaining = new int[45 - 2];
        int read = reader.readValues(remaining, 0, remaining.length);
        assertEquals (remaining.length, read);
        file.close();
    }
}
