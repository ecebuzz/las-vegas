package edu.brown.lasvegas.qe.parse;

import org.junit.Test;

import edu.brown.lasvegas.qe.parser.QueryParser;

/**
 * Testcases for {@link QueryParser}.
 */
public class QueryParserTest {
    @Test
    public void testParse () throws Exception {
        QueryParser parser = new QueryParser();
        parser.parse("SELECT lo_orderkey,lo_quantity FROM LINEORDER WHERE lo_commitdate<19970101");
        // BETWEEN is not implemented yet as of Hive 0.8.0
        // parser.parse("SELECT lo_orderkey,lo_quantity FROM LINEORDER WHERE lo_commitdate BETWEEN 19960817 AND 19970101");
    }
}
