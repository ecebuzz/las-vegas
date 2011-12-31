package edu.brown.lasvegas.traits;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.RawValueReader;
import edu.brown.lasvegas.lvfs.RawValueWriter;

/**
 * Functor to read/write fixed-length java objects and primitive type arrays.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public interface FixLenValueTraits<T extends Comparable<T>, AT> extends ValueTraits<T, AT> {
    
    /**
     * Reads arbitrary number of values at once.
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values read
     */
    int readValues (RawValueReader reader, AT buffer, int off, int len) throws IOException;
    
    /**
     * Writes arbitrary number of values at once.
     * @param writer destination to write out
     * @param values the values to write out
     * @param off offset of the values
     * @param len number of values to write
     * @throws IOException
     */
    void writeValues (RawValueWriter writer, AT values, int off, int len) throws IOException;

    /**
     * Returns the number of bits to represent one value.
     */
    short getBitsPerValue ();
}
