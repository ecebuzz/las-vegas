package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.tuple.BufferedTupleWriter;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.TextFileTupleReader;

/**
 * Testcases for {@link PartitionMergerForSameScheme}.
 */
public class PartitionMergerForSameSchemeTest {
    private static final Logger LOG = Logger.getLogger(PartitionMergerForSameSchemeTest.class);
    private VirtualFile tmpFolder;
    private final static int FRACTURES = 2;
    private ColumnFileBundle[][] columnFiles;
    private ColumnType[] columnTypes;
    private CompressionType[] compressions;

    @Before
    public void setUp () throws Exception {
        tmpFolder = new LocalVirtualFile("test/merge/");
        if (tmpFolder.exists()) {
            tmpFolder.delete(true);
        }
        tmpFolder.mkdirs();
        if (!tmpFolder.exists()) {
            throw new IOException ("can't make a temporary folder: " + tmpFolder);
        }
        columnTypes = MiniLineorder.getScheme();
        compressions = MiniLineorder.getDefaultCompressions();
    }
    private void createColumnFiles (int sortColumn) throws Exception {
        columnFiles = new ColumnFileBundle[FRACTURES][];
        for (int rep = 0; rep < FRACTURES; ++rep) {
            {
                TextFileTupleReader reader = MiniLineorder.open();
                String[] names = new String[compressions.length];
                for (int i = 0; i < names.length; ++i) names[i] = "tmp_" + rep + "_" + i;
                BufferedTupleWriter writer = new BufferedTupleWriter(reader, 1 << 10, tmpFolder, compressions, names, false);
                int appended = writer.appendAllTuples();
                assertEquals (45, appended);
                columnFiles[rep] = writer.finish();
                writer.close();
                reader.close();
            }
            
            {
                String[] newNames = new String[compressions.length];
                for (int i = 0; i < newNames.length; ++i) newNames[i] = "sorted_" + rep + "_" + i;
                PartitionRewriter rewriter = new PartitionRewriter(tmpFolder, columnFiles[rep], newNames, compressions, sortColumn);
                columnFiles[rep] = rewriter.execute();
            }
        }
    }


    @Test
    public void testOrderKeySort () throws Exception {
        createColumnFiles (0); // lo_orderkey
        String[] newNames = new String[compressions.length];
        for (int i = 0; i < newNames.length; ++i) newNames[i] = "merged_" + i;
        PartitionMergerForSameScheme merger = new PartitionMergerForSameScheme(tmpFolder, columnFiles, newNames, columnTypes, compressions, 0);
        ColumnFileBundle[] mergedFiles = merger.execute();
        checkMergedFiles (mergedFiles, 0, "testOrderKeySort");
    }


    @Test
    public void testPrioritySort () throws Exception {
        createColumnFiles (6); // lo_orderpriority
        String[] newNames = new String[compressions.length];
        for (int i = 0; i < newNames.length; ++i) newNames[i] = "merged_" + i;
        PartitionMergerForSameScheme merger = new PartitionMergerForSameScheme(tmpFolder, columnFiles, newNames, columnTypes, compressions, 6);
        ColumnFileBundle[] mergedFiles = merger.execute();
        checkMergedFiles (mergedFiles, 6, "testPrioritySort");
    }

    @Test
    public void testOrderDateSort () throws Exception {
        createColumnFiles (5); // lo_orderdate
        String[] newNames = new String[compressions.length];
        for (int i = 0; i < newNames.length; ++i) newNames[i] = "merged_" + i;
        PartitionMergerForSameScheme merger = new PartitionMergerForSameScheme(tmpFolder, columnFiles, newNames, columnTypes, compressions, 5);
        ColumnFileBundle[] mergedFiles = merger.execute();
        checkMergedFiles (mergedFiles, 5, "testOrderDateSort");
    }

    @Test
    public void testNoSort () throws Exception {
        createColumnFiles (0); // lo_orderkey
        String[] newNames = new String[compressions.length];
        for (int i = 0; i < newNames.length; ++i) newNames[i] = "merged_" + i;
        PartitionMergerForSameScheme merger = new PartitionMergerForSameScheme(tmpFolder, columnFiles, newNames, columnTypes, compressions, null);
        ColumnFileBundle[] mergedFiles = merger.execute();
        checkMergedFiles (mergedFiles, null, "testNoSort");
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void checkMergedFiles (ColumnFileBundle[] mergedFiles, Integer sorting, String name) throws IOException {
        ColumnFileTupleReader tupleReader = new ColumnFileTupleReader(mergedFiles);
        assertEquals(MiniLineorder.MINI_LINEORDER_COUNT * FRACTURES, tupleReader.getTupleCount());
        StringBuffer buf = new StringBuffer(8192);
        buf.append(name + " all tuples\r\n");
        Comparable prev = null;
        for (int i = 0; i < tupleReader.getTupleCount(); ++i) {
            boolean nextExists = tupleReader.next();
            assertTrue (nextExists);
            buf.append(tupleReader.getCurrentTupleAsString());
            buf.append("\r\n");

            if (sorting != null) {
                if (i == 0) {
                    prev = (Comparable) tupleReader.getObject(sorting);
                } else {
                    Comparable cur = (Comparable) tupleReader.getObject(sorting);
                    assertTrue(prev.compareTo(cur) <= 0);
                    prev = cur;
                }
            }
        }
        LOG.info(new String(buf));
        tupleReader.close();
    }
}
