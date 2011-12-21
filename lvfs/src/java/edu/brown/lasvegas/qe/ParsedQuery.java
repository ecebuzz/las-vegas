package edu.brown.lasvegas.qe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.io.Writable;

/**
 * Represents a syntax tree of a compiled query.
 */
public final class ParsedQuery implements Writable {
    /** ID of the query. */
    private int queryId;
    /** The original SQL command. */
    private String sql;
    /** a serialized syntax tree of the query. Only used to receive the syntax tree over the wire. */
    private String dumpedTree;
    /** The actual antlr tree. This is NOT a Writable object, so we just dump it to String (dumpedTree) on serialization. */
    private transient ASTNode parsedTree;
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(queryId);
        out.writeBoolean(sql == null);
        if (sql != null) {
            out.writeUTF(sql);
        }
        if (parsedTree != null) {
            dumpedTree = parsedTree.dump();
        }
        out.writeBoolean(dumpedTree == null);
        if (dumpedTree != null) {
            out.writeUTF(dumpedTree);
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        queryId = in.readInt();
        if (in.readBoolean()) {
            sql = null;
        } else {
            sql = in.readUTF();
        }
        if (in.readBoolean()) {
            dumpedTree = null;
        } else {
            dumpedTree = in.readUTF();
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static ParsedQuery read (DataInput in) throws IOException {
        ParsedQuery obj = new ParsedQuery();
        obj.readFields(in);
        return obj;
    }
    
    @Override
    public String toString() {
        return "ParsedQuery-" + queryId + ", SQL='" + sql + "'" + ", parsedTree="
            + (parsedTree == null ? "NULL" : parsedTree.dump());
    }
    
    /**
     * Gets the iD of the query.
     *
     * @return the iD of the query
     */
    public int getQueryId() {
        return queryId;
    }
    
    /**
     * Sets the iD of the query.
     *
     * @param queryId the new iD of the query
     */
    public void setQueryId(int queryId) {
        this.queryId = queryId;
    }
    
    /**
     * Gets the original SQL command.
     *
     * @return the original SQL command
     */
    public String getSql() {
        return sql;
    }
    
    /**
     * Sets the original SQL command.
     *
     * @param sql the new original SQL command
     */
    public void setSql(String sql) {
        this.sql = sql;
    }
    
    /**
     * Gets the a serialized syntax tree of the query.
     *
     * @return the a serialized syntax tree of the query
     */
    public String getDumpedTree() {
        return dumpedTree;
    }
    
    /**
     * Sets the a serialized syntax tree of the query.
     *
     * @param dumpedTree the new a serialized syntax tree of the query
     */
    public void setDumpedTree(String dumpedTree) {
        this.dumpedTree = dumpedTree;
    }
    
    /**
     * Gets the actual antlr tree.
     *
     * @return the actual antlr tree
     */
    public ASTNode getParsedTree() {
        return parsedTree;
    }
    
    /**
     * Sets the actual antlr tree.
     *
     * @param parsedTree the new actual antlr tree
     */
    public void setParsedTree(ASTNode parsedTree) {
        this.parsedTree = parsedTree;
    }
}
