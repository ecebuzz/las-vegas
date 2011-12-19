package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;
import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.util.URLVirtualFile;

/** helper methods to use min_lineorder.tbl in this package. */
public final class MiniLineorder {
    static TextFileTableScheme getScheme() {
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
        return scheme;
    }
    static TextFileTableReader open() throws IOException {
        URL testFile = MiniLineorder.class.getResource("mini_lineorder.tbl");
        TextFileTableScheme scheme = getScheme();
        TextFileTableReader reader = new TextFileTableReader(new URLVirtualFile(testFile), scheme, "|");
        return reader;
    }

}
