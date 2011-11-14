package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.*;

import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.LVColumnTypes;
import edu.brown.lasvegas.LVTableScheme;

/**
 * Testcase for {@link TextFileTableReader}.
 * This one tests all functions with alltypes.csv. 
 */
public class TextFileTableReaderTest {
    private TextFileTableReader reader;
    
    @Before
    public void setUp() throws Exception {
        InputStream testFile = this.getClass().getResourceAsStream("alltypes.csv");
        LVTableScheme scheme = new LVTableScheme("alltypes");
        scheme.addColumn("col0", LVColumnTypes.BOOLEAN)
            .addColumn("col1", LVColumnTypes.TINYINT)
            .addColumn("col2", LVColumnTypes.SMALLINT)
            .addColumn("col3", LVColumnTypes.INTEGER)
            .addColumn("col4", LVColumnTypes.BIGINT)
            .addColumn("col5", LVColumnTypes.FLOAT)
            .addColumn("col6", LVColumnTypes.DOUBLE)
            .addColumn("col7", LVColumnTypes.DATE)
            .addColumn("col8", LVColumnTypes.TIME)
            .addColumn("col9", LVColumnTypes.TIMESTAMP)
            .addColumn("col10", LVColumnTypes.VARCHAR)
            ;
        
        reader = new TextFileTableReader(testFile, scheme, ",");
    }

    @After
    public void tearDown() throws Exception {
        reader.close();
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#next()}.
     */
    @Test
    public void testNext() throws Exception {
        assertTrue(reader.next());
        assertTrue(reader.next());
        assertFalse(reader.next());
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getScheme()}.
     */
    @Test
    public void testGetScheme() {
        assertEquals("col3", reader.getScheme().getColumnName(3));
        assertEquals(LVColumnTypes.BOOLEAN, reader.getScheme().getColumnType(0));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#wasNull()}.
     */
    @Test
    public void testWasNull() throws Exception {
        assertTrue(reader.next());
        reader.getInt(3);
        assertFalse(reader.wasNull());
        assertTrue(reader.next());
        reader.getInt(3);
        assertFalse(reader.wasNull());
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getBoolean(int)}.
     */
    @Test
    public void testGetBoolean() throws Exception {
        assertTrue(reader.next());
        assertFalse(reader.getBoolean(0));
        assertTrue(reader.next());
        assertTrue(reader.getBoolean(0));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getByte(int)}.
     */
    @Test
    public void testGetByte() throws Exception {
        assertTrue(reader.next());
        assertEquals((byte) -50, reader.getByte(1));
        assertTrue(reader.next());
        assertEquals((byte) 60, reader.getByte(1));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getShort(int)}.
     */
    @Test
    public void testGetShort() throws Exception {
        assertTrue(reader.next());
        assertEquals(-4332, reader.getShort(2));
        assertTrue(reader.next());
        assertEquals(7332, reader.getShort(2));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getInt(int)}.
     */
    @Test
    public void testGetInt() throws Exception {
        assertTrue(reader.next());
        assertEquals(-531232356, reader.getInt(3));
        assertTrue(reader.next());
        assertEquals(531232356, reader.getInt(3));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getLong(int)}.
     */
    @Test
    public void testGetLong() throws Exception {
        assertTrue(reader.next());
        assertEquals(-23940928390482431L, reader.getLong(4));
        assertTrue(reader.next());
        assertEquals(23940928390482431L, reader.getLong(4));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getFloat(int)}.
     */
    @Test
    public void testGetFloat() throws Exception {
        assertTrue(reader.next());
        assertEquals(-0.312444f, reader.getFloat(5), 0.0000001f);
        assertTrue(reader.next());
        assertEquals(0.312444f, reader.getFloat(5), 0.0000001f);
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getDouble(int)}.
     */
    @Test
    public void testGetDouble() throws Exception {
        assertTrue(reader.next());
        assertEquals(-0.00314159265358979d, reader.getDouble(6), 0.00000000001d);
        assertTrue(reader.next());
        assertEquals(3.14159265358979d, reader.getDouble(6), 0.00000000001d);
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getSqlDate(int)}.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testGetSqlDate() throws Exception {
        assertTrue(reader.next());
        assertEquals(new java.sql.Date(2011 - 1900, 11 - 1, 20), reader.getSqlDate(7));
        assertTrue(reader.next());
        assertEquals(new java.sql.Date(2011 - 1900, 11 - 1, 1), reader.getSqlDate(7));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getSqlTime(int)}.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testGetSqlTime() throws Exception {
        assertTrue(reader.next());
        assertEquals(new java.sql.Time(12, 34, 56), reader.getSqlTime(8));
        assertTrue(reader.next());
        assertEquals(new java.sql.Time(12, 44, 56), reader.getSqlTime(8));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getSqlTimestamp(int)}.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testGetSqlTimestamp() throws Exception {
        assertTrue(reader.next());
        assertEquals(new java.sql.Timestamp(2011 - 1900, 11 - 1, 20, 12, 34, 56, 987 * 1000000), reader.getSqlTimestamp(9));
        assertTrue(reader.next());
        assertEquals(new java.sql.Timestamp(2011 - 1900, 2 - 1, 20, 12, 34, 56, 987 * 1000000), reader.getSqlTimestamp(9));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getString(int)}.
     */
    @Test
    public void testGetString() throws Exception {
        assertTrue(reader.next());
        assertEquals("aaabcd", reader.getString(10));
        assertTrue(reader.next());
        assertEquals("bbbb", reader.getString(10));
    }

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#getObject(int)}.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testGetObject() throws Exception {
        Object[][] correctAnswers = new Object[2][];
        correctAnswers[0] = new Object[] {
            new Boolean(false), new Byte((byte) -50), new Short((short)-4332), new Integer(-531232356), new Long(-23940928390482431L),
            new Float(-0.312444f), new Double(-0.00314159265358979d),
            new java.sql.Date(2011 - 1900, 11 - 1, 20), new java.sql.Time(12, 34, 56), new java.sql.Timestamp(2011 - 1900, 11 - 1, 20, 12, 34, 56, 987 * 1000000),
            "aaabcd"
        };
        correctAnswers[1] = new Object[] {
            new Boolean(true), new Byte((byte) 60), new Short((short) 7332), new Integer(531232356), new Long(23940928390482431L),
            new Float(0.312444f), new Double(3.14159265358979),
            new java.sql.Date(2011 - 1900, 11 - 1, 1), new java.sql.Time(12, 44, 56), new java.sql.Timestamp(2011 - 1900, 2 - 1, 20, 12, 34, 56, 987 * 1000000),
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

    /**
     * Test method for {@link edu.brown.lasvegas.lvfs.imp.TextFileTableReader#toString()}.
     */
    @Test
    public void testToString() {
        reader.toString();
    }
}
