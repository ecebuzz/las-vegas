package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;
import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.util.URLVirtualFile;

/** helper methods to use min_lineorder.tbl in this package. */
public final class MiniLineorder {
    static ColumnType[] getScheme() {
        return new ColumnType[]{
            /*"lo_orderkey", */ColumnType.INTEGER,
            /*"lo_linenumber", */ColumnType.TINYINT,
            /*"lo_custkey", */ColumnType.INTEGER,
            /*"lo_partkey", */ColumnType.INTEGER,
            /*"lo_suppkey",*/ ColumnType.INTEGER,
            /*"lo_orderdate",*/ ColumnType.INTEGER,
            /*"lo_orderpriority",*/ ColumnType.VARCHAR,
            /*"lo_shippriority",*/ ColumnType.VARCHAR,
            /*"lo_quantity",*/ ColumnType.INTEGER,
            /*"lo_extendedprice",*/ ColumnType.BIGINT,
            /*"lo_ordertotalprice",*/ ColumnType.INTEGER,
            /*"lo_discount",*/ ColumnType.SMALLINT,
            /*"lo_revenue",*/ ColumnType.BIGINT,
            /*"lo_supplycost",*/ ColumnType.INTEGER,
            /*"lo_tax",*/ ColumnType.INTEGER,
            /*"lo_commitdate",*/ ColumnType.INTEGER,
            /*"lo_shipmode",*/ ColumnType.VARCHAR,
        };
    }
    static TextFileTupleReader open() throws IOException {
        URL testFile = MiniLineorder.class.getResource("mini_lineorder.tbl");
        ColumnType[] scheme = getScheme();
        TextFileTupleReader reader = new TextFileTupleReader(new VirtualFile[]{new URLVirtualFile(testFile)}, scheme, "|");
        return reader;
    }

}
