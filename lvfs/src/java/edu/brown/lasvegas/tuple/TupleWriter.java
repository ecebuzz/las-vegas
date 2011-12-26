package edu.brown.lasvegas.tuple;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface to write a set of column data (Tuple).
 * A tuple might be a complete set of columns in a table,
 * or might be its subset (projection).
 */
public interface TupleWriter extends Closeable {
    /**
     * Called once before all the other method calls to prepare for the writes.
     * @param reader the interface to provide tuples
     * @throws IOException
     */
    public void init (TupleReader reader) throws IOException;
    
    /**
     * Retrieves one tuple from the reader and appends it to the column files.
     * @throws IOException if reader doesn't have any more tuple to provide, etc
     */
    public void appendTuple () throws IOException;

    /**
     * Retrieves all tuples from the reader and appends them to the column files.
     * @param int the number of tuples written
     * @throws IOException
     */
    public int appendAllTuples () throws IOException;

    /**
     * Retrieves the specified number of tuples from the reader and appends them to the column files.
     * @param maxTuples the number of tuples to write
     * @param int the number of tuples written (might be smaller than maxTuples)
     * @throws IOException
     */
    public int appendTuples (int maxTuples) throws IOException;

    /**
     * flushes the underlying stream.
     * @param sync whether to make the written data all the way down to the disk, calling getFD().sync().
     * @throws IOException
     */
    public void flush(boolean sync) throws IOException;

    /**
     * Finish up writing and write footer for the column files.
     * This method should be called only once at the end before close().
     * Failing to call this method results in corrupted files without footer.
     * @throws IOException
     */
    void writeFileFooter () throws IOException;

    /**
     * Returns the total number of tuples written to this writer.
     */
    int getTupleCount () throws IOException;
}
