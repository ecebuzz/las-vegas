package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.tuple.TextFileTupleReader;

/**
 * Another test using lineorder.tbl.
 */
public class TextFileTupleReaderLineorderTest {
    @Test
    public void testAll() throws Exception {
        ColumnType[] scheme = MiniLineorder.getScheme();
        TextFileTupleReader reader = MiniLineorder.open();
        assertEquals (17, scheme.length);
        assertEquals (scheme.length, reader.getColumnCount());
        int linesRead = 0;
        while (reader.next()) {
            ++linesRead;
            for (int i = 0; i < scheme.length; ++i) {
                assertTrue(reader.getObject(i) != null);
                if (i == 0) {
                    assertTrue (reader.getInteger(i) > 0);
                    assertTrue (reader.getInteger(i) < 200);
                }
                if (i == 1) {
                    assertTrue (reader.getTinyint(i) > (byte) 0);
                    assertTrue (reader.getTinyint(i) < (byte) 10);
                }
                if (i == 5 || i == 15) {
                    assertTrue (reader.getInteger(i) >= 19920101);
                    assertTrue (reader.getInteger(i) <= 19991231);
                }
                if (i == 11) {
                    assertTrue (reader.getSmallint(i) >= 0);
                    assertTrue (reader.getSmallint(i) < 100);
                }
                if (i == 12) {
                    assertTrue (reader.getBigint(i) > 0L);
                }
                if (i == 6 || i == 7 || i == 16) {
                    assertTrue (reader.getVarchar(i).length() > 0);
                }
            }
        }
        
        assertEquals (45, linesRead);
        
        
        reader.close();
    }
}
