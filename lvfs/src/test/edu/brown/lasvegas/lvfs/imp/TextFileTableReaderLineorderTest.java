package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URL;

import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.util.URLVirtualFile;

/**
 * Another test using lineorder.tbl.
 */
public class TextFileTableReaderLineorderTest {
    @Test
    public void testAll() throws Exception {
        URL testFile = this.getClass().getResource("mini_lineorder.tbl");
        
        TextFileTableScheme scheme = new TextFileTableScheme();
        scheme.addColumn(/*"lo_orderkey", */ColumnType.INTEGER)
            .addColumn(/*"lo_linenumber", */ColumnType.TINYINT)
            .addColumn(/*"lo_custkey", */ColumnType.INTEGER)
            .addColumn(/*"lo_partkey", */ColumnType.INTEGER)
            .addColumn(/*"lo_suppkey",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_orderdate",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_orderpriority",*/ ColumnType.VARCHAR)
            .addColumn(/*"lo_shippriority",*/ ColumnType.VARCHAR)
            .addColumn(/*"lo_quantity",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_extendedprice",*/ ColumnType.BIGINT)
            .addColumn(/*"lo_ordertotalprice",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_discount",*/ ColumnType.SMALLINT)
            .addColumn(/*"lo_revenue",*/ ColumnType.BIGINT)
            .addColumn(/*"lo_supplycost",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_tax",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_commitdate",*/ ColumnType.INTEGER)
            .addColumn(/*"lo_shipmode",*/ ColumnType.VARCHAR)
            ;
        
        TextFileTableReader reader = new TextFileTableReader(new URLVirtualFile(testFile), scheme, "|");
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
