package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

/**
 * A reader that provides methods to read typed values.
 * @param <T> Value type
 * @param <AT> Array type 
 */
public abstract class LocalTypedReader<T, AT> extends LocalRawFileReader {
    protected LocalTypedReader(File rawFile) throws IOException {
        super (rawFile);
    }

    /**
     * Reads and returns the next entry.
     */
    public abstract T readValue () throws IOException;
    
    /**
     * Reads arbitrary number of values at once.
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values read
     */
    public abstract int readValues (AT buffer, int off, int len) throws IOException;

    /**
     * Skip one entry.
     */
    public abstract void skipValue () throws IOException;

    /**
     * Skip arbitrary number of entries.
     * NOTE: depending on the implementation class, this might be inefficient.
     * Use index files and {@link #seekToByteAbsolute(long)} to speed-up jumps.
     * @param skip number of entries to skip. must be positive values.
     */
    public abstract void skipValues (int skip) throws IOException;
}