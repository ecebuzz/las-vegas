package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Another test using lineorder.tbl.
 */
public class TextFileTableReaderLineorderTest {
    @Test
    public void testAll() throws Exception {
        TextFileTableScheme scheme = MiniLineorder.getScheme();
        TextFileTableReader reader = MiniLineorder.open();
        assertEquals (17, scheme.getColumnCount());
        assertEquals (scheme.getColumnCount(), reader.getColumnCount());
        System.out.println("reading:\n" + scheme.toString());
        int linesRead = 0;
        while (reader.next()) {
            ++linesRead;
            for (int i = 0; i < scheme.getColumnCount(); ++i) {
                assertTrue(reader.getString(i).length() > 0);
                assertTrue(reader.getObject(i) != null);
                if (i == 0) {
                    assertTrue (reader.getLong(i) > 0);
                    assertTrue (reader.getLong(i) < 200);
                }
                if (i == 1) {
                    assertTrue (reader.getLong(i) > 0);
                    assertTrue (reader.getLong(i) < 10);
                    assertTrue (reader.getByte(i) > (byte) 0);
                    assertTrue (reader.getByte(i) < (byte) 10);
                }
                if (i == 5 || i == 15) {
                    assertTrue (reader.getInt(i) >= 19920101);
                    assertTrue (reader.getInt(i) <= 19991231);
                }
            }
        }
        
        assertEquals (45, linesRead);
        
        
        reader.close();
    }
}
