package edu.brown.lasvegas.lvfs;

import java.io.IOException;

/**
 * The additional writer methods for RLE-compressed file.
 */
public interface TypedRLEWriter<T extends Comparable<T>, AT> extends TypedWriter<T, AT> {
    /**
     * Close the current run and start a new run with the given value
     * @param value new value to start the new run
     * @param newRunLength the initial run-length of new run. usually 1, but might be more in batched write.
     * @return the newly started run
     * @throws IOException
     */
    ValueRun<T> startNewRun (T value, int newRunLength) throws IOException;

    /**
     * Returns the current run of the values being compressed.
     */
    ValueRun<T> getCurrentRun ();
    
    /**
     * Returns the number of runs we have so far output.
     * After writeFileFooter(), this gives the exact number of runs in this file.
     */
    int getRunCount ();
}
