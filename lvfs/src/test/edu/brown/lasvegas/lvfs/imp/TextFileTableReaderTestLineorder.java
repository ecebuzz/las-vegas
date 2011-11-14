package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.*;
import org.junit.Test;
import java.io.InputStream;

import edu.brown.lasvegas.LVColumnTypes;
import edu.brown.lasvegas.LVTableScheme;

/**
 * Another test using lineorder.tbl.
 */
public class TextFileTableReaderTestLineorder {
    @Test
    public void testAll() throws Exception {
        InputStream testFile = this.getClass().getResourceAsStream("mini_lineorder.tbl");
        LVTableScheme scheme = new LVTableScheme("mini_lineorder");
        scheme.addColumn("lo_orderkey", LVColumnTypes.INTEGER)
            .addColumn("lo_linenumber", LVColumnTypes.TINYINT)
            .addColumn("lo_custkey", LVColumnTypes.INTEGER)
            .addColumn("lo_partkey", LVColumnTypes.INTEGER)
            .addColumn("lo_suppkey", LVColumnTypes.INTEGER)
            .addColumn("lo_orderdate", LVColumnTypes.INTEGER)
            .addColumn("lo_orderpriority", LVColumnTypes.VARCHAR)
            .addColumn("lo_shippriority", LVColumnTypes.VARCHAR)
            .addColumn("lo_quantity", LVColumnTypes.INTEGER)
            .addColumn("lo_extendedprice", LVColumnTypes.BIGINT)
            .addColumn("lo_ordertotalprice", LVColumnTypes.INTEGER)
            .addColumn("lo_discount", LVColumnTypes.SMALLINT)
            .addColumn("lo_revenue", LVColumnTypes.BIGINT)
            .addColumn("lo_supplycost", LVColumnTypes.INTEGER)
            .addColumn("lo_tax", LVColumnTypes.INTEGER)
            .addColumn("lo_commitdate", LVColumnTypes.INTEGER)
            .addColumn("lo_shipmode", LVColumnTypes.VARCHAR)
            ;
        
        TextFileTableReader reader = new TextFileTableReader(testFile, scheme, "|");
        assertEquals (17, scheme.getColumnCount());
        assertEquals (scheme.getColumnCount(), reader.getScheme().getColumnCount());
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
                if (scheme.getColumnName(i).equals("lo_orderdate") || scheme.getColumnName(i).equals("lo_commitdate")) {
                    assertTrue (reader.getInt(i) >= 19920101);
                    assertTrue (reader.getInt(i) <= 19991231);
                }
            }
        }
        
        assertEquals (45, linesRead);
        
        
        reader.close();
    }
}
