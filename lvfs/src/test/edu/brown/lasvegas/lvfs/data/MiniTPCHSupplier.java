package edu.brown.lasvegas.lvfs.data;

import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;

/** helper methods to use mini_tpch_supplier.tbl in this package. */
public final class MiniTPCHSupplier extends MiniDataSource {
    public ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"0: s_suppkey", */ColumnType.INTEGER,
            /*"1: s_name", */ColumnType.VARCHAR,
            /*"2: s_address",*/ ColumnType.VARCHAR,
            /*"3: s_nationkey",*/ ColumnType.INTEGER,
            /*"4: s_phone",*/ ColumnType.VARCHAR,
            /*"5: s_acctbal",*/ ColumnType.DOUBLE,
            /*"6: s_comment",*/ ColumnType.VARCHAR,
        };
    }
    @Override
    public URL getFileURL() {
        return getClass().getResource("mini_tpch_supplier.tbl");
    }
    
    @Override
    public CompressionType[] getDefaultCompressions() {
        return new CompressionType[]{
            /*"0: s_suppkey", */CompressionType.NONE,
            /*"1: s_name", */CompressionType.NONE,
            /*"2: s_address",*/ CompressionType.NONE,
            /*"3: s_nationkey",*/ CompressionType.NONE,
            /*"4: s_phone",*/ CompressionType.NONE,
            /*"5: s_acctbal",*/ CompressionType.NONE,
            /*"6: s_comment",*/ CompressionType.NONE,
        };
    }
    @Override
    public String[] getColumnNames () {
        return new String[]{
	        "s_suppkey",
	        "s_name",
	        "s_address",
	        "s_nationkey",
	        "s_phone",
	        "s_acctbal",
	        "s_comment",
        };
    }
    @Override
    public int getCount() {
        return 36;
    }
}
