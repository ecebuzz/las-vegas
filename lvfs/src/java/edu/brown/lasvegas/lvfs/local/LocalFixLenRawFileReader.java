package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * LocalRawFileReader that assumes fixed-length entries.
 * Simpler and faster.
 * Integer/float/datetime files without compression, dictionary compressed files,
 * and prefix-compressed integer/float/datetime files fall into this category.
 * @param <T> Value type (e.g., Integer)
 * @param <AT> Array type (e.g., int[]). used for fast batch accesses. 
 */
public class LocalFixLenRawFileReader<T, AT> extends LocalRawFileReader implements TypedReader<T, AT>{
    private static Logger LOG = Logger.getLogger(LocalFixLenRawFileReader.class);

    /**
     * number of bits to represent one entry. so far must be multiply of 8 (might allow 1/2/4 later..)
     */
    private final short bitsPerValue;
    
    private final FixLenValueTraits<T, AT> traits;
    
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalFixLenRawFileReader<Byte, byte[]> getInstanceTinyint(File rawFile) throws IOException {
        return new LocalFixLenRawFileReader<Byte, byte[]>(rawFile, new AllValueTraits.TinyintValueTraits());
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalFixLenRawFileReader<Short, short[]> getInstanceSmallint(File rawFile) throws IOException {
        return new LocalFixLenRawFileReader<Short, short[]>(rawFile, new AllValueTraits.SmallintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalFixLenRawFileReader<Integer, int[]> getInstanceInteger(File rawFile) throws IOException {
        return new LocalFixLenRawFileReader<Integer, int[]>(rawFile, new AllValueTraits.IntegerValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalFixLenRawFileReader<Long, long[]> getInstanceBigint(File rawFile) throws IOException {
        return new LocalFixLenRawFileReader<Long, long[]>(rawFile, new AllValueTraits.BigintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalFixLenRawFileReader<Float, float[]> getInstanceFloat(File rawFile) throws IOException {
        return new LocalFixLenRawFileReader<Float, float[]>(rawFile, new AllValueTraits.FloatValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalFixLenRawFileReader<Double, double[]> getInstanceDouble(File rawFile) throws IOException {
        return new LocalFixLenRawFileReader<Double, double[]>(rawFile, new AllValueTraits.DoubleValueTraits());
    }

    /**
     * @param bitsPerEntry number of bits to represent one entry.
     * so far must be multiply of 8 (might allow 1/2/4 later..).
     */
    private LocalFixLenRawFileReader(File rawFile, FixLenValueTraits<T, AT> traits) throws IOException {
        super (rawFile);
        this.bitsPerValue = traits.getBitsPerValue();
        this.traits = traits;
    }

    /**
     * Jump to the specified absolute tuple position.
     */
    public void seekToTupleAbsolute (int tuple) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("seeking to " + tuple + "th tuple..");
        }
        seekToByteAbsolute((long) bitsPerValue * (long) tuple / 8L);
    }

    /**
     * Jump to the specified tuple position relative to current position.
     */
    public void seekToTupleRelative (int tuple) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("jumping over " + tuple + " tuples..");
        }
        seekToByteRelative((long) bitsPerValue * (long) tuple / 8L);
    }
    
    @Override
    public T readValue() throws IOException {
        return traits.readValue(this);
    }
    @Override
    public void skipValue() throws IOException {
        seekToTupleRelative(1);
    }
}
