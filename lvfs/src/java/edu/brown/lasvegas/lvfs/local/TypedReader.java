package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

/**
 * Defines value read API.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public interface TypedReader<T, AT> {
    /**
     * Reads and returns the next entry.
     */
    T readValue () throws IOException;
    
    /**
     * Skip one entry.
     */
    void skipValue () throws IOException;
}
