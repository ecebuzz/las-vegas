package edu.brown.lasvegas.tuple;

import java.io.IOException;

/**
 * TupleReader that can provide random samples over the data.
 * Such TupleReader can be used to create statistics and design partitions.
 */
public interface SampleableTupleReader extends TupleReader {
    /**
     * This method takes random samples of the data and sets it to
     * the given buffer.
     * @param buffer the buffer to receive the samples.
     * this method tries to exactly fill the buffer, but might end up
     * with a few less tuples than the buffer size.
     * @return the number of sampled tuples.
     * @throws IOException
     */
    int sample (TupleBuffer buffer) throws IOException;

}
