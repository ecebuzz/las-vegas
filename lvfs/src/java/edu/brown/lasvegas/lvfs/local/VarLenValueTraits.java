package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

/**
 * Functor to read/write variable-length java objects.
 */
public abstract class VarLenValueTraits<T> {
    /**
     * Reads one value from the given stream.
     */
    public abstract T readValue (LocalRawFileReader reader, int length) throws IOException;
    
    /** Use this if you don't care about types. */
    public Object readValueAsObject(LocalRawFileReader reader, int length) throws IOException {
        return readValue (reader, length);
    }
}
