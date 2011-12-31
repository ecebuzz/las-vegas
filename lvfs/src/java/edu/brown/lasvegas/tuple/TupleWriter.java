package edu.brown.lasvegas.tuple;

import java.io.Closeable;
import java.io.IOException;

import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.data.PartitionRewriter;

/**
 * Interface to write a set of column data (Tuple) received from {@link TupleReader}.
 * <p>
 * A tuple might be a complete set of columns in a table,
 * or might be its subset (projection).
 * </p>
 * <p>
 * This writer sequentially appends tuples in the order received from the reader.
 * It does NOT sort the column files. Sorting is efficiently done later by {@link PartitionRewriter}.
 * </p>
 */
public interface TupleWriter extends Closeable {
    /**
     * Retrieves all tuples from the reader and appends them to the column files.
     * @param int the number of tuples written
     * @throws IOException
     */
    public int appendAllTuples () throws IOException;

    /**
     * Finish up writing and write footer for the column files.
     * This method should be called only once at the end before close().
     * Failing to call this method results in corrupted files without footer.
     * @throws IOException
     * @return properties of the column files written by the writer.
     * This return value makes sense only when the implementation class writes out columnar files,
     * which is always true so far.

     */
    ColumnFileBundle[] finish () throws IOException;

    /**
     * Returns the total number of tuples written to this writer.
     */
    int getTupleCount () throws IOException;
    
    /** Returns the number of columns. */
    int getColumnCount ();
}
