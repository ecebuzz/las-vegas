package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.util.URLVirtualFile;

/**
 * Testcase for {@link TextFileTupleReader}.
 * This one tests all functions with alltypes.csv. 
 */
public class TextFileTableReaderTest {
    private TextFileTupleReader reader;
    
    @Before
    public void setUp() throws Exception {
        URL testFile = this.getClass().getResource("alltypes.csv");
        ColumnType[] scheme = new ColumnType[] {
            ColumnType.BOOLEAN,
            ColumnType.TINYINT,
            ColumnType.SMALLINT,
            ColumnType.INTEGER,
            ColumnType.BIGINT,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.DATE,
            ColumnType.TIME,
            ColumnType.TIMESTAMP,
            ColumnType.VARCHAR,
        };
        
        reader = new TextFileTupleReader(new VirtualFile[]{new URLVirtualFile(testFile)}, scheme, ",");
    }

    @After
    public void tearDown() throws Exception {
        reader.close();
    }

    @Test
    public void testNext() throws Exception {
        assertTrue(reader.next());
        assertTrue(reader.next());
        assertFalse(reader.next());
    }

    @Test
    public void testGetBoolean() throws Exception {
        assertTrue(reader.next());
        assertFalse(reader.getBoolean(0));
        assertTrue(reader.next());
        assertTrue(reader.getBoolean(0));
    }

    @Test
    public void testGetByte() throws Exception {
        assertTrue(reader.next());
        assertEquals((byte) -50, reader.getTinyint(1));
        assertTrue(reader.next());
        assertEquals((byte) 60, reader.getTinyint(1));
    }

    @Test
    public void testGetShort() throws Exception {
        assertTrue(reader.next());
        assertEquals(-4332, reader.getSmallint(2));
        assertTrue(reader.next());
        assertEquals(7332, reader.getSmallint(2));
    }

    @Test
    public void testGetInt() throws Exception {
        assertTrue(reader.next());
        assertEquals(-531232356, reader.getInteger(3));
        assertTrue(reader.next());
        assertEquals(531232356, reader.getInteger(3));
    }

    @Test
    public void testGetLong() throws Exception {
        assertTrue(reader.next());
        assertEquals(-23940928390482431L, reader.getBigint(4));
        assertTrue(reader.next());
        assertEquals(23940928390482431L, reader.getBigint(4));
    }

    @Test
    public void testGetFloat() throws Exception {
        assertTrue(reader.next());
        assertEquals(-0.312444f, reader.getFloat(5), 0.0000001f);
        assertTrue(reader.next());
        assertEquals(0.312444f, reader.getFloat(5), 0.0000001f);
    }

    @Test
    public void testGetDouble() throws Exception {
        assertTrue(reader.next());
        assertEquals(-0.00314159265358979d, reader.getDouble(6), 0.00000000001d);
        assertTrue(reader.next());
        assertEquals(3.14159265358979d, reader.getDouble(6), 0.00000000001d);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetSqlDate() throws Exception {
        assertTrue(reader.next());
        assertEquals(new java.sql.Date(2011 - 1900, 11 - 1, 20), reader.getDate(7));
        assertTrue(reader.next());
        assertEquals(new java.sql.Date(2011 - 1900, 11 - 1, 1), reader.getDate(7));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetSqlTime() throws Exception {
        assertTrue(reader.next());
        assertEquals(new java.sql.Time(12, 34, 56), reader.getTime(8));
        assertTrue(reader.next());
        assertEquals(new java.sql.Time(12, 44, 56), reader.getTime(8));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetSqlTimestamp() throws Exception {
        assertTrue(reader.next());
        assertEquals(new java.sql.Timestamp(2011 - 1900, 11 - 1, 20, 12, 34, 56, 987 * 1000000), reader.getTimestamp(9));
        assertTrue(reader.next());
        assertEquals(new java.sql.Timestamp(2011 - 1900, 2 - 1, 20, 12, 34, 56, 987 * 1000000), reader.getTimestamp(9));
    }

    @Test
    public void testGetString() throws Exception {
        assertTrue(reader.next());
        assertEquals("aaabcd", reader.getVarchar(10));
        assertTrue(reader.next());
        assertEquals("bbbb", reader.getVarchar(10));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testGetObject() throws Exception {
        Object[][] correctAnswers = new Object[2][];
        correctAnswers[0] = new Object[] {
            new Byte((byte)0), new Byte((byte) -50), new Short((short)-4332), new Integer(-531232356), new Long(-23940928390482431L),
            new Float(-0.312444f), new Double(-0.00314159265358979d),
            new java.sql.Date(2011 - 1900, 11 - 1, 20).getTime(), new java.sql.Time(12, 34, 56).getTime(), new java.sql.Timestamp(2011 - 1900, 11 - 1, 20, 12, 34, 56, 987 * 1000000).getTime(),
            "aaabcd"
        };
        correctAnswers[1] = new Object[] {
            new Byte((byte)1), new Byte((byte) 60), new Short((short) 7332), new Integer(531232356), new Long(23940928390482431L),
            new Float(0.312444f), new Double(3.14159265358979),
            new java.sql.Date(2011 - 1900, 11 - 1, 1).getTime(), new java.sql.Time(12, 44, 56).getTime(), new java.sql.Timestamp(2011 - 1900, 2 - 1, 20, 12, 34, 56, 987 * 1000000).getTime(),
            "bbbb"
        };
        for (int i = 0; i < 2; ++i) {
            assertTrue(reader.next());
            for (int j = 0; j < correctAnswers[i].length; ++j) {
                Object ans = correctAnswers[i][j];
                Object val = reader.getObject(j);
                String msg = "failed: i=" + i + ",j=" + j + ", ans=" + ans + ",val=" + val;
                if (ans instanceof Float) {
                    assertTrue(val instanceof Float);
                    assertEquals(msg, ((Float) ans).floatValue(), ((Float) val).floatValue(), 0.0000001f);
                } else if (ans instanceof Double) {
                    assertTrue(val instanceof Double);
                    assertEquals(msg, ((Double) ans).doubleValue(), ((Double) val).doubleValue(), 0.00000000001d);
                } else {
                    assertEquals(msg, ans, val);
                }
            }
        }
    }

    @Test
    public void testToString() {
        reader.toString();
    }
}
