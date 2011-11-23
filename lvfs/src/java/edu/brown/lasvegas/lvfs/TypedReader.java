package edu.brown.lasvegas.lvfs;

import java.io.IOException;

/**
 * A reader that provides methods to read typed values.
 * This object allows per-tuple and tuple-aware operations unlike {@link RawValueReader}.
 * However, also unlike {@link RawValueReader}, this object does not provide
 * raw operations such as readBytes() and skipBytes() which will break how tuple is
 * managed in this object.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public interface TypedReader<T, AT> {
    /**
     * Reads and returns the next entry.
     * This method should not be used frequently unless
     * you will read a very small number of values.
     * Otherwise, use readValues() with a large buffer size.
     */
    T readValue () throws IOException;
    
    /**
     * Reads arbitrary number of values at once.
     * This method significantly reduces per-value overheads
     * such as Disk I/O and de-serialization. Use this method
     * with large buffer as much as possible. 
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values read
     */
    int readValues (AT buffer, int off, int len) throws IOException;

    /**
     * Skip one entry.
     */
    void skipValue () throws IOException;

    /**
     * Skip arbitrary number of entries.
     * NOTE: depending on the implementation class, this might be inefficient.
     * Use index files and {@link #seekToByteAbsolute(long)} to speed-up jumps.
     * @param skip number of entries to skip. must be positive values.
     */
    void skipValues (int skip) throws IOException;

    /**
     * Close the file handle and release all resources.
     */
    void close() throws IOException;

    /**
     * Returns the total number of tuples in this file.
     */
    int getTotalTuples ();
}