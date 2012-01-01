package edu.brown.lasvegas.tuple;

import java.io.Closeable;
import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.util.ByteArray;

/**
 * Interface to read a set of column data (Tuple).
 * A tuple might be a complete set of columns in a table,
 * or might be its subset (projection).
 */
public interface TupleReader extends Closeable {
    /**
     * This method is called to read a tuple, including the first tuple.
     * @return whether there is a tuple to return.
     * @see #nextBatch(TupleBuffer)
     */
    boolean next() throws IOException;
    
    /**
     * This method reads multiple tuples at once and appends the tuples
     * to the given buffer. In many implementations, this will be much
     * more efficient than {@link #next()}.
     * @param buffer the buffer to append the tuples read from this reader.
     * if the buffer becomes full, this method stops reading tuples.
     * @return the number of tuples read and appended to the buffer. -1 if
     * the reader reaches the end.
     * @throws IOException
     */
    int nextBatch (TupleBuffer buffer) throws IOException;
    
    /**
     * Returns a string representation of current tuple.
     * If this reader is reading a text file, this method should respect the original format,
     * ie returning the actual line it read.
     */
    String getCurrentTupleAsString ();
    
    /** Returns the number of columns. */
    int getColumnCount();

    /** Returns the data type of specified column. */
    ColumnType getColumnType(int columnIndex);
    /** Returns the data type of all columns. */
    ColumnType[] getColumnTypes();

    /** for general reads. */
    Object getObject (int columnIndex) throws IOException;

    /**
     * Reads a boolean column value.
     * this is only for convenience. Internally boolean is stored as byte.
     * So, batched version is also the TINYINT method.
     */
    boolean getBoolean (int columnIndex) throws IOException;
    
    /** Reads a TINYINT column value. Consider using batched version for better performance. */
    byte getTinyint (int columnIndex) throws IOException;

    /** Reads a SMALLINT column value. Consider using batched version for better performance. */
    short getSmallint (int columnIndex) throws IOException;

    /** Reads a INTEGER column value. Consider using batched version for better performance. */
    int getInteger (int columnIndex) throws IOException;

    /** Reads a BIGINT column value. Consider using batched version for better performance. */
    long getBigint (int columnIndex) throws IOException;

    /** Reads a FLOAT column value. Consider using batched version for better performance. */
    float getFloat (int columnIndex) throws IOException;

    /** Reads a DOUBLE column value. Consider using batched version for better performance. */
    double getDouble (int columnIndex) throws IOException;

    /** Reads a VARCHAR column value. Consider using batched version for better performance. */
    String getVarchar (int columnIndex) throws IOException;

    /** Reads a VARBIN column value. Consider using batched version for better performance. */
    ByteArray getVarbin (int columnIndex) throws IOException;


    /**
     * Reads a DATE column value.
     * this is only for convenience. Internally it is stored as long.
     * So, batched version is also the BIGINT method.
     */
    java.sql.Date getDate (int columnIndex) throws IOException;
    /**
     * Reads a TIME column value.
     * this is only for convenience. Internally it is stored as long.
     * So, batched version is also the BIGINT method.
     */
    java.sql.Time getTime (int columnIndex) throws IOException;
    /**
     * Reads a TIMESTAMP column value.
     * this is only for convenience. Internally it is stored as long.
     * So, batched version is also the BIGINT method.
     */
    java.sql.Timestamp getTimestamp (int columnIndex) throws IOException;
}
