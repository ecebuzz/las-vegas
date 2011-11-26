package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.util.List;


/**
 * Functor to read/write java objects and their arrays.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public interface ValueTraits<T, AT> {
    /**
     * Reads one value from the given stream.
     */
    T readValue (RawValueReader reader) throws IOException;
    
    /**
     * Writes one value. This method should be mainly used for testcases as it'd be slow.
     */
    void writeValue (RawValueWriter writer, T value) throws IOException;


    /**
     * Scan the array and splits it to run-lengthes. Used to apply RLE.
     * The implementation of this method might be much faster
     * when AT is not merely T[] but a primitive array because we can avoid
     * creating wrapper objects in the case.
     * @param results extracted RunLength objects are appended to this list
     * @param values the values to compress
     * @param off offset of the values
     * @param len number of values to compress
     */
    void extractRunLengthes (List<ValueRun<T>> results, AT values, int off, int len);
    
    /**
     * Sets the value to the array at once. Used to efficiently get values
     * from RLE-compressed file. However, even more efficient way to access
     * RLE-compressed file is the {@link TypedRLEReader#getCurrentRun()}.
     * @param value the value to be set
     * @param array the array to receive the value
     * @param off the offset of the array to receive the value
     * @param len the number of elements of the array to receive the value
     */
    void fillArray (T value, AT array, int off, int len);
}
