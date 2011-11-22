package edu.brown.lasvegas.lvfs;

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
    public abstract T readValue (RawValueReader reader) throws IOException;
    
    /** Use this if you don't care about types. */
    public Object readValueAsObject(RawValueReader reader) throws IOException {
        return readValue (reader);
    }
    
    /**
     * Reads arbitrary number of values at once.
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values read
     */
    public abstract int readValues (RawValueReader reader, AT buffer, int off, int len) throws IOException;

    /**
     * Returns the number of bits to represent one value.
     */
    public abstract short getBitsPerValue ();
    
    /**
     * Writes one value. This method should be mainly used for testcases as it'd be slow.
     */
    public abstract void writeValue (RawValueWriter writer, T value) throws IOException;
    
    /**
     * Writes arbitrary number of values at once.
     * @param writer destination to write out
     * @param values the values to write out
     * @param off offset of the values
     * @param len number of values to write
     * @throws IOException
     */
    public abstract void writeValues (RawValueWriter writer, AT values, int off, int len) throws IOException;

    protected byte[] conversionBuffer = new byte[1024];
    protected int readIntoConversionBuffer(RawValueReader reader, int len) throws IOException {
        int bytesPerValue = getBitsPerValue() / 8;
        reserveConversionBufferSize (len);
        int read = reader.readBytes(conversionBuffer, 0, len * bytesPerValue);
        return read / bytesPerValue;
    }
    protected int reserveConversionBufferSize(int len) {
        int bytesPerValue = getBitsPerValue() / 8;
        if (len * bytesPerValue > conversionBuffer.length) {
            conversionBuffer = new byte[len * bytesPerValue];
        }
        return len * bytesPerValue;
    }
}
