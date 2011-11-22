package edu.brown.lasvegas.lvfs;

import java.io.IOException;


/**
 * Functor to read/write variable-length java objects.
 */
public abstract class VarLenValueTraits<T> {
    /**
     * Reads one value from the given stream.
     */
    public abstract T readValue (RawValueReader reader) throws IOException;
    
    /** Use this if you don't care about types. */
    public final Object readValueAsObject(RawValueReader reader) throws IOException {
        return readValue (reader);
    }

    /**
     * Skip one entry.
     */
    public final void skipValue (RawValueReader reader) throws IOException {
        int length = reader.readLengthHeader();
        reader.skipBytes(length);
    }
    
    /**
     * Writes one value. This method should be mainly used for testcases as it'd be slow.
     */
    public abstract void writeValue (RawValueWriter writer, T value) throws IOException;
}
