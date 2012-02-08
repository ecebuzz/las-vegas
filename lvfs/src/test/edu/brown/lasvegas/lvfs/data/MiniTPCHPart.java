package edu.brown.lasvegas.lvfs.data;

import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;

/** helper methods to use mini_tpch_part.tbl in this package. */
public final class MiniTPCHPart extends MiniDataSource {
    public ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"0: p_partkey", */ColumnType.INTEGER,
            /*"1: p_name", */ColumnType.VARCHAR,
            /*"2: p_mfgr",*/ ColumnType.VARCHAR,
            /*"3: p_brand",*/ ColumnType.VARCHAR,
            /*"4: p_type",*/ ColumnType.VARCHAR,
            /*"5: p_size",*/ ColumnType.INTEGER,
            /*"6: p_container",*/ ColumnType.VARCHAR,
            /*"7: p_retailprice",*/ ColumnType.FLOAT,
            /*"8: p_comment",*/ ColumnType.VARCHAR,
        };
    }
    @Override
    public URL getFileURL() {
        return getClass().getResource("mini_tpch_part.tbl");
    }
    
    @Override
    public CompressionType[] getDefaultCompressions() {
        return new CompressionType[]{
            /*"0: p_partkey", */CompressionType.NONE,
            /*"1: p_name", */CompressionType.NONE,
            /*"2: p_mfgr",*/ CompressionType.DICTIONARY,
            /*"3: p_brand",*/ CompressionType.DICTIONARY,
            /*"4: p_type",*/ CompressionType.DICTIONARY,
            /*"5: p_size",*/ CompressionType.NONE,
            /*"6: p_container",*/ CompressionType.DICTIONARY,
            /*"7: p_retailprice",*/ CompressionType.NONE,
            /*"8: p_comment",*/ CompressionType.NONE,
        };
    }
    @Override
    public String[] getColumnNames () {
        return new String[]{
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
        };
    }
    @Override
    public int getCount() {
        return 36;
    }
}
