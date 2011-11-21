package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

/**
 * Functor to read/write fixed-length java objects and primitive type arrays.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public abstract class FixLenValueTraits<T, AT> {
    /**
     * Reads one value from the given stream.
     */
    public abstract T readValue (LocalRawFileReader reader) throws IOException;
    
    /** Use this if you don't care about types. */
    public Object readValueAsObject(LocalRawFileReader reader) throws IOException {
        return readValue (reader);
    }
    
    /**
     * Reads arbitrary number of values at once.
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values read
     */
    public abstract int readValues (LocalRawFileReader reader, AT buffer, int off, int len) throws IOException;

    /**
     * Returns the number of bits to represent one value.
     */
    public abstract short getBitsPerValue ();

    protected byte[] conversionBuffer = new byte[1024];
    protected int readIntoConversionBuffer(LocalRawFileReader reader, int len) throws IOException {
        int bytesPerValue = getBitsPerValue() / 8;
        int remaining = (int) ((reader.getRawFileSize() - reader.getCurPosition()) / bytesPerValue);
        if (len > remaining) {
            len = remaining;
        }
        if (len * bytesPerValue > conversionBuffer.length) {
            conversionBuffer = new byte[len * bytesPerValue];
        }
        int read = reader.readBytes(conversionBuffer, 0, len * bytesPerValue);
        assert (read == len * bytesPerValue);
        return len;
    }
}