package edu.brown.lasvegas.lvfs.data;

import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;

/** helper methods to use min_lineorder.tbl in this package. */
public final class MiniSSBLineorder extends MiniDataSource {
    @Override
    public ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"0: lo_orderkey", */ColumnType.INTEGER,
            /*"1: lo_linenumber", */ColumnType.TINYINT,
            /*"2: lo_custkey", */ColumnType.INTEGER,
            /*"3: lo_partkey", */ColumnType.INTEGER,
            /*"4: lo_suppkey",*/ ColumnType.INTEGER,
            /*"5: lo_orderdate",*/ ColumnType.INTEGER,
            /*"6: lo_orderpriority",*/ ColumnType.VARCHAR,
            /*"7: lo_shippriority",*/ ColumnType.VARCHAR,
            /*"8: lo_quantity",*/ ColumnType.INTEGER,
            /*"9: lo_extendedprice",*/ ColumnType.BIGINT,
            /*"10: lo_ordertotalprice",*/ ColumnType.INTEGER,
            /*"11: lo_discount",*/ ColumnType.SMALLINT,
            /*"12: lo_revenue",*/ ColumnType.BIGINT,
            /*"13: lo_supplycost",*/ ColumnType.INTEGER,
            /*"14: lo_tax",*/ ColumnType.INTEGER,
            /*"15: lo_commitdate",*/ ColumnType.INTEGER,
            /*"16: lo_shipmode",*/ ColumnType.VARCHAR,
        };
    }
    @Override
    public URL getFileURL() {
        return MiniSSBLineorder.class.getResource("mini_lineorder.tbl");
    }
    
    @Override
    public CompressionType[] getDefaultCompressions() {
        return new CompressionType[]{
            /*"lo_orderkey", */CompressionType.NONE,
            /*"lo_linenumber", */CompressionType.NONE,
            /*"lo_custkey", */CompressionType.NONE,
            /*"lo_partkey", */CompressionType.NONE,
            /*"lo_suppkey",*/ CompressionType.NONE,
            /*"lo_orderdate",*/ CompressionType.NONE,
            /*"lo_orderpriority",*/ CompressionType.DICTIONARY,
            /*"lo_shippriority",*/ CompressionType.DICTIONARY,
            /*"lo_quantity",*/ CompressionType.NONE,
            /*"lo_extendedprice",*/ CompressionType.NONE,
            /*"lo_ordertotalprice",*/ CompressionType.NONE,
            /*"lo_discount",*/ CompressionType.NONE,
            /*"lo_revenue",*/ CompressionType.NONE,
            /*"lo_supplycost",*/ CompressionType.NONE,
            /*"lo_tax",*/ CompressionType.NONE,
            /*"lo_commitdate",*/ CompressionType.NONE,
            /*"lo_shipmode",*/ CompressionType.DICTIONARY,
        };
    }
    @Override
    public String[] getColumnNames () {
        return new String[]{
            "lo_orderkey", 
            "lo_linenumber", 
            "lo_custkey", 
            "lo_partkey", 
            "lo_suppkey",
            "lo_orderdate",
            "lo_orderpriority",
            "lo_shippriority",
            "lo_quantity",
            "lo_extendedprice",
            "lo_ordertotalprice",
            "lo_discount",
            "lo_revenue",
            "lo_supplycost",
            "lo_tax",
            "lo_commitdate",
            "lo_shipmode",
        };
    }
    
    @Override
    public int getCount() {
        return 45;
    }
}
