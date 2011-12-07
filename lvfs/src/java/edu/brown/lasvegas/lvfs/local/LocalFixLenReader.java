package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * File reader that assumes fixed-length entries.
 * Simpler and faster.
 * Integer/float/datetime files without compression, dictionary compressed files,
 * and prefix-compressed integer/float/datetime files fall into this category.
 * @param <T> Value type (e.g., Integer)
 * @param <AT> Array type (e.g., int[]). used for fast batch accesses. 
 */
public final class LocalFixLenReader<T, AT> extends LocalTypedReaderBase<T, AT>{
    private static Logger LOG = Logger.getLogger(LocalFixLenReader.class);

    /**
     * number of bits to represent one entry. so far must be multiply of 8 (might allow 1/2/4 later..)
     */
    private final short bitsPerValue;
    
    private final FixLenValueTraits<T, AT> traits;
    
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalFixLenReader<Byte, byte[]> getInstanceTinyint(VirtualFile rawFile) throws IOException {
        return new LocalFixLenReader<Byte, byte[]>(rawFile, new AllValueTraits.TinyintValueTraits());
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalFixLenReader<Short, short[]> getInstanceSmallint(VirtualFile rawFile) throws IOException {
        return new LocalFixLenReader<Short, short[]>(rawFile, new AllValueTraits.SmallintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalFixLenReader<Integer, int[]> getInstanceInteger(VirtualFile rawFile) throws IOException {
        return new LocalFixLenReader<Integer, int[]>(rawFile, new AllValueTraits.IntegerValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalFixLenReader<Long, long[]> getInstanceBigint(VirtualFile rawFile) throws IOException {
        return new LocalFixLenReader<Long, long[]>(rawFile, new AllValueTraits.BigintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalFixLenReader<Float, float[]> getInstanceFloat(VirtualFile rawFile) throws IOException {
        return new LocalFixLenReader<Float, float[]>(rawFile, new AllValueTraits.FloatValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalFixLenReader<Double, double[]> getInstanceDouble(VirtualFile rawFile) throws IOException {
        return new LocalFixLenReader<Double, double[]>(rawFile, new AllValueTraits.DoubleValueTraits());
    }

    public LocalFixLenReader(VirtualFile rawFile, FixLenValueTraits<T, AT> traits, int streamBufferSize) throws IOException {
        super (rawFile, traits, streamBufferSize);
        this.bitsPerValue = traits.getBitsPerValue();
        this.traits = traits;
    }
    public LocalFixLenReader(VirtualFile rawFile, FixLenValueTraits<T, AT> traits) throws IOException {
        this (rawFile, traits, 1 << 16);
    }

    /**
     * Jump to the specified absolute tuple position.
     */
    public void seekToTupleAbsolute (int tuple) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("seeking to " + tuple + "th tuple..");
        }
        getRawReader().seekToByteAbsolute((long) bitsPerValue * (long) tuple / 8L);
    }

    /**
     * Jump to the specified tuple position relative to current position.
     */
    public void seekToTupleRelative (int tuple) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("jumping over " + tuple + " tuples..");
        }
        getRawReader().seekToByteRelative((long) bitsPerValue * (long) tuple / 8L);
    }
    
    @Override
    public T readValue() throws IOException {
        return traits.readValue(getRawValueReader());
    }
    @Override
    public int readValues(AT buffer, int off, int len) throws IOException {
        return traits.readValues(getRawValueReader(), buffer, off, len);
    }
    @Override
    public void skipValue() throws IOException {
        seekToTupleRelative(1);
    }
    @Override
    public void skipValues(int skip) throws IOException {
        seekToTupleRelative(skip);
    }
    
    @Override
    public int getTotalTuples() {
        return (int) (getRawReader().getRawFileSize() * 8 / bitsPerValue);
    }
}
