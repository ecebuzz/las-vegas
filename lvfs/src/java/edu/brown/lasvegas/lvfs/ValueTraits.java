package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import edu.brown.lasvegas.util.KeyValueArrays;

/**
 * Functor to read/write java objects and their arrays.
 * @param <T> Value type
 * @param <AT> Array type. This might not be T[] but the primitive array for speed (that's why we need this class).
 */
public interface ValueTraits<T extends Comparable<T>, AT> {
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
     * Creates an array of the specified size.
     */
    AT createArray (int size);
    
    /** Returns the length of the array. */
    int length (AT array);
    
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
    
    /**
     * Converts the given collection to an array. When T[]!=AT (e.g., Long/long[]), this will produce
     * a more compact representation of the values. Otherwise, it's same as {@link Collection#toArray()}.
     */
    AT toArray (Collection<T> values);


    /**
     * Call binarySearch() in Arrays.
     * @see Arrays
     */
    int binarySearch (AT array, T value);
    
    /**
     * Call sort() in Arrays.
     * @see Arrays
     */
    void sort (AT keys);

    /**
     * Call sort() in Arrays.
     * @see Arrays
     */
    void sort (AT keys, int fromIndex, int toIndex);
    
    
    /**
     * Call sort() in {@link KeyValueArrays}.
     * @see KeyValueArrays
     */
    void sortKeyValue (AT keys, int[] values);

    /**
     * Call sort() in KeyValueArrays.
     * @see KeyValueArrays
     */
    void sortKeyValue (AT keys, int[] values, int fromIndex, int toIndex);

    /**
     * Deserializes an array from byte buffer. This method is a batched read and supposed
     * to be as low-overhead as possible.
     * @param buffer byte buffer to read an array from
     * @return deserialized array 
     */
    AT deserializeArray (ByteBuffer buffer) throws IOException;

    /**
     * Serializes an array and writes it out to byte buffer.
     * @param array the array to write out.
     * @param buffer byte buffer to write out the array. use {@link #getSerializedByteSize(Object)}
     * to know the required size.
     * @param number of bytes written
     */
    int serializeArray (AT array, ByteBuffer buffer);
    /**
     * Returns the required bytes to serialize the given array. 
     */
    int getSerializedByteSize (AT array);
}
