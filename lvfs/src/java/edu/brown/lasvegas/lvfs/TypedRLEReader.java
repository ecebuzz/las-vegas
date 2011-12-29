package edu.brown.lasvegas.lvfs;

import java.io.IOException;

/**
 * The additional reader methods for RLE-compressed file.
 * These can be used for in-situ query execution. 
 */
public interface TypedRLEReader<T extends Comparable<T>, AT> extends TypedReader<T, AT> {
    /**
     * Returns the current run of the compressed values.
     * The run might not start from the (conceptual) current tuple. 
     * @return the run of values which contain the current tuple
     * @throws IOException
     */
    ValueRun<T> getCurrentRun () throws IOException;

    /**
     * Moves the current tuple to the beginning of next run and returns it.
     * @return the next run. null if the current run was the last run
     * @throws IOException
     */
    ValueRun<T> getNextRun () throws IOException;
}
