package edu.brown.lasvegas.lvfs.data;

import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;

public final class MiniTPCHOrders extends MiniDataSource {
	/*
	CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
            O_CUSTKEY        INTEGER NOT NULL,
            O_ORDERSTATUS    CHAR(1) NOT NULL,
            O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
            O_ORDERDATE      DATE NOT NULL,
            O_ORDERPRIORITY  CHAR(15) NOT NULL,  
            O_CLERK          CHAR(15) NOT NULL, 
            O_SHIPPRIORITY   INTEGER NOT NULL,
            O_COMMENT        VARCHAR(79) NOT NULL);
    */

    public ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"0: o_orderkey", */ColumnType.BIGINT,
            /*"1: o_custkey", */ColumnType.INTEGER,
            /*"2: o_orderstatus",*/ ColumnType.VARCHAR,
            /*"3: o_totalprice",*/ ColumnType.DOUBLE,
            /*"4: o_orderdate",*/ ColumnType.DATE,
            /*"5: o_orderpriority",*/ ColumnType.VARCHAR,
            /*"6: o_clerk",*/ ColumnType.VARCHAR,
            /*"7: o_shippriority",*/ ColumnType.TINYINT,
            /*"8: o_comment",*/ ColumnType.VARCHAR,
        };
    }
    @Override
    public URL getFileURL() {
        return getClass().getResource("mini_tpch_orders.tbl");
    }
    
    @Override
    public CompressionType[] getDefaultCompressions() {
        return new CompressionType[]{
            /*"0: o_orderkey", */CompressionType.NONE,
            /*"1: o_custkey", */CompressionType.NONE,
            /*"2: o_orderstatus",*/ CompressionType.DICTIONARY,
            /*"3: o_totalprice",*/ CompressionType.NONE,
            /*"4: o_orderdate",*/ CompressionType.NONE,
            /*"5: o_orderpriority",*/ CompressionType.DICTIONARY,
            /*"6: o_clerk",*/ CompressionType.NONE/*SNAPPY*/,
            /*"7: o_shippriority",*/ CompressionType.NONE,
            /*"8: o_comment",*/ CompressionType.NONE/*SNAPPY*/,
        };
    }
    @Override
    public String[] getColumnNames () {
        return new String[]{
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        };
    }
    @Override
    public int getCount() {
        return 33;
    }
}
