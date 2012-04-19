package edu.brown.lasvegas.lvfs.data;

import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;

public final class MiniTPCHCustomer extends MiniDataSource {
/*
	CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL);
*/
	public ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"0: c_custkey", */ColumnType.INTEGER,
            /*"1: c_name", */ColumnType.VARCHAR,
            /*"2: c_address",*/ ColumnType.VARCHAR,
            /*"3: c_nationkey",*/ ColumnType.INTEGER,
            /*"4: c_phone",*/ ColumnType.VARCHAR,
            /*"5: c_acctbal",*/ ColumnType.DOUBLE,
            /*"6: c_mktsegment",*/ ColumnType.VARCHAR,
            /*"7: c_comment",*/ ColumnType.VARCHAR,
        };
    }
    @Override
    public URL getFileURL() {
        return getClass().getResource("mini_tpch_customer.tbl");
    }
    
    @Override
    public CompressionType[] getDefaultCompressions() {
        return new CompressionType[]{
            /*"0: c_custkey", */CompressionType.NONE,
            /*"1: c_name", */CompressionType.NONE,
            /*"2: c_address",*/ CompressionType.NONE,
            /*"3: c_nationkey",*/ CompressionType.NONE,
            /*"4: c_phone",*/ CompressionType.NONE,
            /*"5: c_acctbal",*/ CompressionType.NONE,
            /*"6: c_mktsegment",*/ CompressionType.DICTIONARY,
            /*"7: c_comment",*/ CompressionType.NONE,
        };
    }
    @Override
    public String[] getColumnNames () {
        return new String[]{
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        };
    }
    @Override
    public int getCount() {
        return 40;
    }
}
