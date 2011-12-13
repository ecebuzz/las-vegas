package edu.brown.lasvegas.lvfs;

import java.io.IOException;

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
     * Scan the array and writes out run-lengthes in it. Used to apply RLE.
     * The implementation of this method might be much faster
     * when AT is not merely T[] but a primitive array because we can avoid
     * creating wrapper objects in the case.
     * <p>Note, to shorten the implementation, this function assumes there IS
     * a current run (not the first tuple). The caller has to make sure there
     * is a run.</p>
     * @param writer writer object to receive the value runs
     * @param values the values to compress
     * @param off offset of the values
     * @param len number of values to compress
     */
    void writeRunLengthes (TypedRLEWriter<T, AT> writer, AT values, int off, int len) throws IOException;
    
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
    
    /**
     * Get from the array (return array[index]). Minimize the use of this function as it's slow.
     */
    T get (AT array, int index);

    /**
     * Put to the array (array[index]=value). Minimize the use of this function as it's slow.
     */
    void set (AT array, int index, T value);
}