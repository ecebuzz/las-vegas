package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * File writer that assumes fixed-length entries.
 * Writer is even simpler than Reader as we support only file creation.
 * 
 * This writer doesn't collect position information because fix-len files
 * don't need position indexes.
 * @param <T> Value type (e.g., Integer)
 * @param <AT> Array type (e.g., int[]).
 */
public class LocalFixLenWriter<T, AT> {
    private static Logger LOG = Logger.getLogger(LocalFixLenReader.class);

    private final File file;
    private final FixLenValueTraits<T, AT> traits;
    
    private FileOutputStream stream;
    
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalFixLenWriter<Byte, byte[]> getInstanceTinyint(File rawFile) throws IOException {
        return new LocalFixLenWriter<Byte, byte[]>(rawFile, new AllValueTraits.TinyintValueTraits());
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalFixLenWriter<Short, short[]> getInstanceSmallint(File rawFile) throws IOException {
        return new LocalFixLenWriter<Short, short[]>(rawFile, new AllValueTraits.SmallintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalFixLenWriter<Integer, int[]> getInstanceInteger(File rawFile) throws IOException {
        return new LocalFixLenWriter<Integer, int[]>(rawFile, new AllValueTraits.IntegerValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalFixLenWriter<Long, long[]> getInstanceBigint(File rawFile) throws IOException {
        return new LocalFixLenWriter<Long, long[]>(rawFile, new AllValueTraits.BigintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalFixLenWriter<Float, float[]> getInstanceFloat(File rawFile) throws IOException {
        return new LocalFixLenWriter<Float, float[]>(rawFile, new AllValueTraits.FloatValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalFixLenWriter<Double, double[]> getInstanceDouble(File rawFile) throws IOException {
        return new LocalFixLenWriter<Double, double[]>(rawFile, new AllValueTraits.DoubleValueTraits());
    }

    public LocalFixLenWriter(File file, FixLenValueTraits<T, AT> traits) throws IOException {
        this.file = file;
        this.traits = traits;
        this.stream = new FileOutputStream(file, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("created fixed-len file:" + file.getAbsolutePath());
        }
    }

    public void flush () throws IOException {
        stream.flush();
        if (LOG.isDebugEnabled()) {
            LOG.debug("flushed fixed-len file:" + file.getAbsolutePath());
        }
    }
    public void close () throws IOException {
        stream.flush();
        stream.close();
        stream = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("closed fixed-len file:" + file.getAbsolutePath());
        }
    }
    
    /**
     * Writes arbitrary number of values at once.
     * @param values the values to write out
     * @param off offset of the values
     * @param len number of values to write
     * @throws IOException
     */
    public void writeValues (AT values, int off, int len) throws IOException {
        traits.writeValues(stream, values, off, len);
    }
}
