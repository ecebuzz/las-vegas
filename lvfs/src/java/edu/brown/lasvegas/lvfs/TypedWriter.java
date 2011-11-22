package edu.brown.lasvegas.lvfs;

import java.io.IOException;

/**
 * A writer that provides methods to write typed values.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public interface TypedWriter<T, AT> {
    /**
     * Writes a single value. Avoid using this,
     * and instead use {@link #writeValues(Object, int, int)} whenever possible.
     * @param value the value to write out
     * @throws IOException
     */
    void writeValue (T value) throws IOException;

    /**
     * Writes arbitrary number of values at once.
     * @param values the values to write out
     * @param off offset of the values
     * @param len number of values to write
     * @throws IOException
     */
    void writeValues (AT values, int off, int len) throws IOException;
}
