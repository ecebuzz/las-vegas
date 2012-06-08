package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.tuple.BufferedTupleWriter;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.FilteredTupleReader;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcases for {@link Repartitioner}.
 */
public class RepartitionerTest {
    private static final Logger LOG = Logger.getLogger(RepartitionerTest.class);
    private LocalVirtualFile tmpFolder;
    private final static int PARTITIONS = 2;
    private ColumnFileBundle[][] columnFiles;
    private ColumnType[] columnTypes;
    private CompressionType[] compressions;

    private final MiniDataSource dataSource = new MiniSSBLineorder();

    @Before
    public void setUp () throws Exception {
        tmpFolder = new LocalVirtualFile("test/repartition/");
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
        // partition by orderkey (<10 and >=10)
        final int ORDERKEY_COLUMN = 0;
        final int BOUNDARY = 10;
        for (int rep = 0; rep < PARTITIONS; ++rep) {
            TextFileTupleReader reader = dataSource.open();
            String[] names = new String[compressions.length];
            for (int i = 0; i < names.length; ++i) names[i] = "tmp_" + rep + "_" + i;
            final boolean firstRep = (rep == 0);
            FilteredTupleReader wrapped = new FilteredTupleReader(reader) {
            	@Override
            	protected boolean isFiltered() {
            		if (firstRep) {
            			return ((Integer) currentData[ORDERKEY_COLUMN]) >= BOUNDARY;
            		} else {
            			return ((Integer) currentData[ORDERKEY_COLUMN]) < BOUNDARY;
            		}
            	}
            };
            BufferedTupleWriter writer = new BufferedTupleWriter(wrapped, 1 << 10, tmpFolder, compressions, names, false);
            int appended = writer.appendAllTuples();
            assertEquals (firstRep ? 23 : 22, appended);
            columnFiles[rep] = writer.finish();
            writer.close();
            reader.close();
        }
    }

    @Test
    public void testRepartitionByLinenumber () throws Exception {
    	int partitioningColumn = 1;
    	ValueRange[] ranges = new ValueRange[]{new ValueRange(ColumnType.TINYINT, null, (byte)4), new ValueRange(ColumnType.TINYINT, (byte) 4, null)};
        Repartitioner repartiotioner = new Repartitioner(tmpFolder, columnFiles, columnTypes, compressions,
        		partitioningColumn, ranges,
        		1 << 7, 1 << 10, 1 << 22);
        LVColumnFile[][] result = repartiotioner.execute();
        checkRepartitionedFiles (result, partitioningColumn, ranges, "linenumber");
    }

    @Test
    public void testRepartitionByLinenumberMultiLevels () throws Exception {
        int partitioningColumn = 1;
        ValueRange[] ranges = new ValueRange[]{
            new ValueRange(ColumnType.TINYINT, null, (byte)1),
            new ValueRange(ColumnType.TINYINT, (byte) 1, (byte)2),
            new ValueRange(ColumnType.TINYINT, (byte) 2, (byte)3),
            new ValueRange(ColumnType.TINYINT, (byte) 3, (byte)4),
            new ValueRange(ColumnType.TINYINT, (byte) 4, (byte)5),
            new ValueRange(ColumnType.TINYINT, (byte) 5, (byte)6),
            new ValueRange(ColumnType.TINYINT, (byte) 6, (byte)7),
            new ValueRange(ColumnType.TINYINT, (byte) 7, null)
        };
        Repartitioner repartiotioner = new Repartitioner(tmpFolder, columnFiles, columnTypes, compressions,
                partitioningColumn, ranges,
                2, 1 << 10, 1 << 22);
        LVColumnFile[][] result = repartiotioner.execute();
        checkRepartitionedFiles (result, partitioningColumn, ranges, "linenumber");
    }

    @Test
    public void testRepartitionByOrderkey () throws Exception {
    	int partitioningColumn = 0;
    	ValueRange[] ranges = new ValueRange[]{new ValueRange(ColumnType.INTEGER, null, 34), new ValueRange(ColumnType.INTEGER, 34, null)};
        Repartitioner repartiotioner = new Repartitioner(tmpFolder, columnFiles, columnTypes, compressions,
        		partitioningColumn, ranges,
        		1 << 7, 1 << 10, 1 << 22);
        LVColumnFile[][] result = repartiotioner.execute();
        checkRepartitionedFiles (result, partitioningColumn, ranges, "orderkey");
    }
    @Test
    public void testRepartitionByOrderkeyMultiLevels () throws Exception {
        int partitioningColumn = 0;
        ArrayList<ValueRange> rangeList = new ArrayList<ValueRange>();
        rangeList.add (new ValueRange(ColumnType.INTEGER, null, 2));
        for (int i = 2; i < 60; i += 2) {
            rangeList.add (new ValueRange(ColumnType.INTEGER, i, i + 2));
        }
        rangeList.add (new ValueRange(ColumnType.INTEGER, 60, null));
        ValueRange[] ranges = rangeList.toArray(new ValueRange[0]);
        Repartitioner repartiotioner = new Repartitioner(tmpFolder, columnFiles, columnTypes, compressions,
                partitioningColumn, ranges,
                3, 1 << 10, 1 << 22);
        LVColumnFile[][] result = repartiotioner.execute();
        checkRepartitionedFiles (result, partitioningColumn, ranges, "orderkey");
    }

    @Test
    public void testRepartitionByOrderpriority () throws Exception {
    	int partitioningColumn = 6;
    	ValueRange[] ranges = new ValueRange[]{
    			new ValueRange(ColumnType.VARCHAR, null, "1"),
    			new ValueRange(ColumnType.VARCHAR, "1", "2"),
    			new ValueRange(ColumnType.VARCHAR, "2", "3"),
    			new ValueRange(ColumnType.VARCHAR, "3", "5"),
    			new ValueRange(ColumnType.VARCHAR, "5", null),
    			};
        Repartitioner repartiotioner = new Repartitioner(tmpFolder, columnFiles, columnTypes, compressions,
        		partitioningColumn, ranges,
        		1 << 7, 1 << 10, 1 << 22);
        LVColumnFile[][] result = repartiotioner.execute();
        checkRepartitionedFiles (result, partitioningColumn, ranges, "orderpriority");
    }
    @Test
    public void testRepartitionByOrderpriorityMultiLevels () throws Exception {
        int partitioningColumn = 6;
        ValueRange[] ranges = new ValueRange[]{
                new ValueRange(ColumnType.VARCHAR, null, "1"),
                new ValueRange(ColumnType.VARCHAR, "1", "2"),
                new ValueRange(ColumnType.VARCHAR, "2", "3"),
                new ValueRange(ColumnType.VARCHAR, "3", "5"),
                new ValueRange(ColumnType.VARCHAR, "5", null),
                };
        Repartitioner repartiotioner = new Repartitioner(tmpFolder, columnFiles, columnTypes, compressions,
                partitioningColumn, ranges,
                2, 1 << 10, 1 << 22);
        LVColumnFile[][] result = repartiotioner.execute();
        checkRepartitionedFiles (result, partitioningColumn, ranges, "orderpriority");
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void checkRepartitionedFiles (LVColumnFile[][] result, int partitioningColumn, ValueRange[] ranges, String name) throws IOException {
    	assertEquals(ranges.length, result.length);
    	int totalCount = 0;
        for (int i = 0; i < result.length; ++i) {
        	if (result[i] == null) {
        		continue;
        	}
            StringBuffer buf = new StringBuffer(8192);
            buf.append(name + " partition(" + i + ") all tuples\r\n");
        	ColumnFileBundle[] bundles = new ColumnFileBundle[columnTypes.length];
            for (int j = 0; j < columnTypes.length; ++j) {
            	assertNotNull(result[i][j]);
            	bundles[j] = new ColumnFileBundle(result[i][j]);
            }
            ColumnFileTupleReader tupleReader = new ColumnFileTupleReader(bundles);
            while (tupleReader.next()) {
                buf.append(tupleReader.getCurrentTupleAsString());
                buf.append("\r\n");
            	++totalCount;
                Comparable cur = (Comparable) tupleReader.getObject(partitioningColumn);
                assertTrue (ranges[i].contains(cur));
            }
            tupleReader.close();
            LOG.info(new String(buf));
        }
        assertEquals(dataSource.getCount(), totalCount);
    }
}
