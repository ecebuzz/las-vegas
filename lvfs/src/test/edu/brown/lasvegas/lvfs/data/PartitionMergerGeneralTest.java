package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.tuple.BufferedTupleWriter;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.FilteredTupleReader;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.tuple.TupleBuffer;

/**
 * Testcases for {@link PartitionMergerGeneral}.
 */
public class PartitionMergerGeneralTest {
    private static final Logger LOG = Logger.getLogger(PartitionMergerGeneralTest.class);
    private VirtualFile tmpFolder;
    private final static int PARTITIONS = 2;
    private ColumnFileBundle[][] columnFiles;
    private ColumnType[] columnTypes;
    private CompressionType[] compressions;
    private SortedMap<Long, Object[]> originalTuples;

    private final MiniDataSource dataSource = new MiniTPCHOrders();

    @Before
    public void setUp () throws Exception {
        tmpFolder = new LocalVirtualFile("test/mergegeneral/");
        if (tmpFolder.exists()) {
            tmpFolder.delete(true);
        }
        tmpFolder.mkdirs();
        if (!tmpFolder.exists()) {
            throw new IOException ("can't make a temporary folder: " + tmpFolder);
        }
        columnTypes = dataSource.getScheme();
        compressions = dataSource.getDefaultCompressions();
        createColumnFiles();
    }
    private void createColumnFiles () throws Exception {
        columnFiles = new ColumnFileBundle[PARTITIONS][];
        // partition by orderpartition (<3-MEDIUM and others)
        for (int rep = 0; rep < PARTITIONS; ++rep) {
            TextFileTupleReader reader = dataSource.open();
            String[] names = new String[compressions.length];
            for (int i = 0; i < names.length; ++i) names[i] = "tmp_" + rep + "_" + i;
            final boolean firstRep = (rep == 0);
            FilteredTupleReader wrapped = new FilteredTupleReader(reader) {
            	@Override
            	protected boolean isFiltered() {
            		if (firstRep) {
            			return ((String) currentData[5]).compareTo("3-MEDIUM") >= 0;
            		} else {
            			return ((String) currentData[5]).compareTo("3-MEDIUM") < 0;
            		}
            	}
            };
            BufferedTupleWriter writer = new BufferedTupleWriter(wrapped, 1 << 10, tmpFolder, compressions, names, false);
            int appended = writer.appendAllTuples();
            assertEquals (firstRep ? 9 : 24, appended);
            columnFiles[rep] = writer.finish();
            writer.close();
            reader.close();
        }

        originalTuples = new TreeMap<Long, Object[]>();
        TextFileTupleReader reader = dataSource.open();
        while (reader.next()) {
        	Object[] array = new Object[reader.getColumnCount()];
        	for (int i = 0; i < reader.getColumnCount(); ++i) {
        		array[i] = reader.getObject(i);
        	}
        	long key = (Long) array[0];
        	assertFalse (originalTuples.containsKey(key));
        	originalTuples.put(key, array);
        }
        reader.close();
    }

    @Test
    public void testMergeNoSort () throws Exception {
    	testMerge(null, "nosort");
    }
    @Test
    public void testMergeOrderkeySort () throws Exception {
    	testMerge(0, "orderkeysort");
    }
    @Test
    public void testMergeCustkeySort () throws Exception {
    	testMerge(1, "custkeysort");
    }
    @Test
    public void testMergeOrdpriSort () throws Exception {
    	testMerge(5, "ordprisort");
    }
    @Test
    public void testMergeClerkSort () throws Exception {
    	testMerge(6, "clerksort");
    }
    private void testMerge (Integer sortColumn, String name) throws Exception {
    	{
	    	PartitionMergerGeneral merger = new PartitionMergerGeneral(columnFiles, columnTypes, sortColumn);
	    	Object[] mergedData = merger.executeOnMemory();
	        checkMergedFiles (mergedData, sortColumn, name);
    	}
    	{
	    	PartitionMergerGeneral merger = new PartitionMergerGeneral(columnFiles, columnTypes, sortColumn);
	        String[] newNames = new String[compressions.length];
	        for (int i = 0; i < newNames.length; ++i) newNames[i] = "merged_" + i;
	    	ColumnFileBundle[] bundles = merger.executeOnDisk(tmpFolder, newNames, compressions);
            for (int i = 0; i < columnTypes.length; ++i) {
            	assertNotNull(bundles[i]);
            }
            ColumnFileTupleReader tupleReader = new ColumnFileTupleReader(bundles);
            TupleBuffer buffer = new TupleBuffer(columnTypes, dataSource.getCount());
            int read = tupleReader.nextBatch(buffer);
            assertEquals (dataSource.getCount(), read);
            assertFalse (tupleReader.next());
        	Object[] readData = new Object[columnTypes.length];
            for (int i = 0; i < columnTypes.length; ++i) {
                readData[i] = buffer.getColumnBuffer(i);
            }
            tupleReader.close();
	        checkMergedFiles (readData, sortColumn, name + "_fromdisk");
    	}
    	
    }

    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void checkMergedFiles (Object[] mergedData, Integer sortColumn, String name) throws IOException {
    	int len = ((ValueTraits) ValueTraitsFactory.getInstance(columnTypes[0])).length(mergedData[0]);
    	assertEquals(originalTuples.size(), len);
    	assertEquals(dataSource.getCount(), len);
        StringBuffer buf = new StringBuffer(8192);
        buf.append(name + " after merge\r\n");
        Comparable prev = null;
        for (int i = 0; i < len; ++i) {
    		long key = ((long[]) mergedData[0])[i];
        	assertTrue(originalTuples.containsKey(key));
        	Object[] originalData = originalTuples.get(key);
        	for (int j = 0; j < columnTypes.length; ++j) {
        		Object val = ((ValueTraits) ValueTraitsFactory.getInstance(columnTypes[j])).get(mergedData[j], i);
            	assertEquals (originalData[j], val);
            	buf.append(val + "|");
            	if (sortColumn != null && sortColumn == j) {
            		if (prev != null) {
            			assertTrue (prev.compareTo(val) <= 0);
            		}
            		prev = (Comparable) val;
            	}
        	}
        	buf.append("\r\n");
        }
        LOG.info(new String(buf));
    }

}
