package edu.brown.lasvegas;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.LasVegasFileSystem;
import edu.brown.lasvegas.lvfs.imp.TextFileTableReader;

/**
 * Provides a way to sequentially read tuple data from some data source.
 * Analogous to JDBC's ResultSet.
 * 
 * Mainly used while importing a local file or a raw HDFS file.
 * If the user program needs to implement its own import program,
 * implement this class and give it to {@link LasVegasFileSystem} to import
 * their own files. But, usually the built-in implementation of this class,
 * {@link TextFileTableReader}, which assumes simple
 * line-delimiters/column-delimiters should suffice.
 * 
 * Note that this interface will not be provided during a usual query
 * processing. Supporting this interface will be quite costly over
 * distributed and partitioned file blocks.
 * @see TextFileTableReader
 */
public interface LVTableReader {
    /**
     * This method is called to read a tuple, including the first tuple.
     * @return whether there is a tuple to return.
     */
    boolean next() throws IOException;
    
    /** Revoke every resource this object holds. */
    void close() throws IOException;
    
    /** Returns the number of columns. */
    int getColumnCount();

    /** Returns the data type of specified column. */
    LVColumnType getColumnType(int columnIndex);
    
    /** for general reads. */
    Object getObject (int columnIndex) throws IOException;

    /** Tells if the last column read had NULL value. */
    boolean wasNull();

    boolean getBoolean (int columnIndex) throws IOException;
    byte getByte(int columnIndex) throws IOException;
    short getShort (int columnIndex) throws IOException;
    int getInt (int columnIndex) throws IOException;
    long getLong (int columnIndex) throws IOException;

    float getFloat (int columnIndex) throws IOException;
    double getDouble (int columnIndex) throws IOException;
    
    String getString (int columnIndex) throws IOException;
    
    java.sql.Date getSqlDate (int columnIndex) throws IOException;
    java.sql.Time getSqlTime (int columnIndex) throws IOException;
    java.sql.Timestamp getSqlTimestamp (int columnIndex) throws IOException;
}
