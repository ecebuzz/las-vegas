package edu.brown.lasvegas.lvfs.data;

import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;

/** helper methods to use mini_tpch_lineitem.tbl in this package. */
public final class MiniTPCHLineitem extends MiniDataSource {
    public ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"0: l_orderkey", */ColumnType.BIGINT, // NOTE: Scale 500 and beyond exceeds int32 limit. So, this has to be BIGINT
            /*"1: l_partkey", */ColumnType.INTEGER,
            /*"2: l_suppkey",*/ ColumnType.INTEGER,
            /*"3: l_linenumber",*/ ColumnType.TINYINT,
            /*"4: l_quantity",*/ ColumnType.FLOAT, // all values are actually integers, but the TPCH spec requests floating points
            /*"5: l_extendedprice",*/ ColumnType.DOUBLE,
            /*"6: l_discount",*/ ColumnType.FLOAT,
            /*"7: l_tax",*/ ColumnType.FLOAT,
            /*"8: l_returnflag",*/ ColumnType.VARCHAR,
            /*"9: l_linestatus",*/ ColumnType.VARCHAR,
            /*"10: l_shipdate",*/ ColumnType.DATE,
            /*"11: l_commitdate",*/ ColumnType.DATE,
            /*"12: l_receiptdate",*/ ColumnType.DATE,
            /*"13: l_shipinstruct",*/ ColumnType.VARCHAR,
            /*"14: l_shipmode",*/ ColumnType.VARCHAR,
            /*"15: l_comment",*/ ColumnType.VARCHAR,
        };
    }
    @Override
    public URL getFileURL() {
        return getClass().getResource("mini_tpch_lineitem.tbl");
    }
    
    @Override
    public CompressionType[] getDefaultCompressions() {
        return new CompressionType[]{
            /*"0: l_orderkey", */CompressionType.NONE,
            /*"1: l_partkey", */CompressionType.NONE,
            /*"2: l_suppkey",*/ CompressionType.NONE,
            /*"3: l_linenumber",*/ CompressionType.NONE,
            /*"4: l_quantity",*/ CompressionType.DICTIONARY,/*CompressionType.NONE*/ // it sounds a bit weird to compress
            /*"5: l_extendedprice",*/ CompressionType.NONE,
            /*"6: l_discount",*/ CompressionType.DICTIONARY,/*CompressionType.NONE*/ // quantity/tax/discount as dictionary,
            /*"7: l_tax",*/ CompressionType.DICTIONARY,/*CompressionType.NONE*/  // but actually they are very few-valued in TPCH data.
            /*"8: l_returnflag",*/ CompressionType.DICTIONARY,
            /*"9: l_linestatus",*/ CompressionType.DICTIONARY,
            /*"10: l_shipdate",*/ CompressionType.DICTIONARY, /*CompressionType.NONE*/
            /*"11: l_commitdate",*/ CompressionType.DICTIONARY,/*CompressionType.NONE*/
            /*"12: l_receiptdate",*/ CompressionType.DICTIONARY,/*CompressionType.NONE*/
            /*"13: l_shipinstruct",*/ CompressionType.DICTIONARY,
            /*"14: l_shipmode",*/ CompressionType.DICTIONARY,
            /*"15: l_comment",*/ CompressionType.GZIP_BEST_COMPRESSION,/*CompressionType.NONE*/
        };
    }
    @Override
    public String[] getColumnNames () {
        return new String[]{
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        };
    }
    @Override
    public int getCount() {
        return 58;
    }
}
