package edu.brown.lasvegas.lvfs;

import java.io.IOException;


/**
 * Functor to read/write variable-length java objects.
 */
public interface VarLenValueTraits<T> {
    /**
     * Reads one value from the given stream.
     */
    T readValue (RawValueReader reader) throws IOException;

    /**
     * Writes one value. This method should be mainly used for testcases as it'd be slow.
     */
    void writeValue (RawValueWriter writer, T value) throws IOException;
}
