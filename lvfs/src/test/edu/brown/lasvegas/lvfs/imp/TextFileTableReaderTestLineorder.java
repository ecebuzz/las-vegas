package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.*;
import org.junit.Test;
import java.io.InputStream;

import edu.brown.lasvegas.LVColumnType;

/**
 * Another test using lineorder.tbl.
 */
public class TextFileTableReaderTestLineorder {
    @Test
    public void testAll() throws Exception {
        InputStream testFile = this.getClass().getResourceAsStream("mini_lineorder.tbl");
        TextFileTableScheme scheme = new TextFileTableScheme();
        scheme.addColumn(/*"lo_orderkey", */LVColumnType.INTEGER)
            .addColumn(/*"lo_linenumber", */LVColumnType.TINYINT)
            .addColumn(/*"lo_custkey", */LVColumnType.INTEGER)
            .addColumn(/*"lo_partkey", */LVColumnType.INTEGER)
            .addColumn(/*"lo_suppkey",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_orderdate",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_orderpriority",*/ LVColumnType.VARCHAR)
            .addColumn(/*"lo_shippriority",*/ LVColumnType.VARCHAR)
            .addColumn(/*"lo_quantity",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_extendedprice",*/ LVColumnType.BIGINT)
            .addColumn(/*"lo_ordertotalprice",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_discount",*/ LVColumnType.SMALLINT)
            .addColumn(/*"lo_revenue",*/ LVColumnType.BIGINT)
            .addColumn(/*"lo_supplycost",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_tax",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_commitdate",*/ LVColumnType.INTEGER)
            .addColumn(/*"lo_shipmode",*/ LVColumnType.VARCHAR)
            ;
        
        TextFileTableReader reader = new TextFileTableReader(testFile, scheme, "|");
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
