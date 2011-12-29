package edu.brown.lasvegas.tuple;

import java.io.Closeable;
import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.ValueRun;
import edu.brown.lasvegas.util.ByteArray;

/**
 * Interface to write a set of column data (Tuple).
 * A tuple might be a complete set of columns in a table,
 * or might be its subset (projection).
 */
public interface TupleReader extends Closeable {
    /**
     * Jump to the specified absolute tuple position.
     * @param tuple the tuple to locate.
     */
    void seekToTupleAbsolute(int tuple) throws IOException;
    
    /**
     * This method is called to read a tuple, including the first tuple.
     * @return whether there is a tuple to return.
     * @see #nextBatch(int, TupleBuffer)
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
    
    /** Returns the current tuple position. */
    int getCurrentTuple () throws IOException;
    /**
     * Returns the total number of tuples this reader can return.
     * Depending on the underlying implementation, this might be slow, or not supported.
     */
    int getTupleCount () throws IOException;
    
    /** Returns the number of columns. */
    int getColumnCount();

    /** Returns the data type of specified column. */
    ColumnType getColumnType(int columnIndex);
    /** Returns the data type of all columns. */
    ColumnType[] getColumnTypes();
    
    /**
     * Returns the compression type of specified column.
     * Users don't have to be aware of the underlying compression type,
     * but this interface also allows to exploit the compression type
     * to speed-up (or even completely bypass) compression/decompression.
     * @see #getDictionary(int)
     * @see #getCurrentRun(int)
     */
    CompressionType getCompressionType (int columnIndex);
    /** Returns the compression type of all columns. */
    CompressionType[] getCompressionTypes ();
    
    /**
     * Returns the dictionary <b>assuming the underlying column data is dictionary compressed</b>.
     * This method is used to exploit the underlying compression.
     */
    OrderedDictionary<?, ?> getDictionary (int columnIndex);
    /**
     * Returns the values before de-compression <b>assuming the underlying column data is dictionary compressed with 1-byte integers (0-256 distinct values)</b>.
     * @see #getDictionary(int)
     */
    int getDictionaryCompressedValuesByte (int columnIndex, byte[] buffer, int off, int len) throws IOException;
    /**
     * Returns the values before de-compression <b>assuming the underlying column data is dictionary compressed with 2-byte integers (257-65536 distinct values)</b>.
     * @see #getDictionary(int)
     */
    int getDictionaryCompressedValuesShort (int columnIndex, short[] buffer, int off, int len) throws IOException;
    /**
     * Returns the values before de-compression <b>assuming the underlying column data is dictionary compressed with 4-byte integers (65537- distinct values)</b>.
     * @see #getDictionary(int)
     */
    int getDictionaryCompressedValuesInt (int columnIndex, int[] buffer, int off, int len) throws IOException;
    
    
    /**
     * Returns the current run of the compressed values <b>assuming the underlying column data is RLE-compressed</b>.
     * The run might not start from the (conceptual) current tuple. 
     * @return the run of values which contain the current tuple
     */
    ValueRun<?> getCurrentRun (int columnIndex);

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
