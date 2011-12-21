package edu.brown.lasvegas.qe.parser;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import edu.brown.lasvegas.qe.ParsedQuery;

public class QueryParser {
    public ParsedQuery parse (String sql) throws Exception {
        ParseDriver hiveParseDriver = new ParseDriver();
        ASTNode astNode = hiveParseDriver.parse(sql);
        System.out.println(astNode.dump());
        return null;
    }
}
