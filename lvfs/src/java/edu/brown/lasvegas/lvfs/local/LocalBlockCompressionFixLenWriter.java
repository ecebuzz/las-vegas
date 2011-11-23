package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;
import edu.brown.lasvegas.lvfs.TypedWriter;

/**
 * Implementation of block-compressed files for fixed-length columns.
 * As this is fixed-length, each block is simply an array of values.
 * No footer at the end of block.
 * @param <T> Value type (e.g., Integer)
 * @param <AT> Array type (e.g., int[]).
 */
public final class LocalBlockCompressionFixLenWriter<T, AT> extends LocalBlockCompressionWriter implements TypedWriter<T, AT> {
    private final FixLenValueTraits<T, AT> traits;
    
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Byte, byte[]> getInstanceTinyint(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Byte, byte[]>(file, compressionType, new AllValueTraits.TinyintValueTraits());
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Short, short[]> getInstanceSmallint(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Short, short[]>(file, compressionType, new AllValueTraits.SmallintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Integer, int[]> getInstanceInteger(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Integer, int[]>(file, compressionType, new AllValueTraits.IntegerValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Long, long[]> getInstanceBigint(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Long, long[]>(file, compressionType, new AllValueTraits.BigintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalBlockCompressionFixLenWriter<Float, float[]> getInstanceFloat(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Float, float[]>(file, compressionType, new AllValueTraits.FloatValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalBlockCompressionFixLenWriter<Double, double[]> getInstanceDouble(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Double, double[]>(file, compressionType, new AllValueTraits.DoubleValueTraits());
    }

    public LocalBlockCompressionFixLenWriter(File file, CompressionType compressionType, FixLenValueTraits<T, AT> traits) throws IOException {
        super (file, compressionType);
        this.traits = traits;
    }

    @Override
    public void writeValues (AT values, int off, int len) throws IOException {
        flushBlockIfNeeded();
        // in case len is really large, we split values to a few blocks
        int threshold = super.blockSizeInKB << 10;
        int curoff = off;
        int curlen = len;
        while (super.currentBlockUsed + curlen * traits.getBitsPerValue() / 8 > threshold) {
            int count = (threshold - super.currentBlockUsed) * 8 /  traits.getBitsPerValue();
            assert (count >= 0);
            if (count > 0) {
                traits.writeValues(getProxyValueWriter(), values, curoff, count);
            }
            curoff += count;
            curlen -= count;
            super.curTuple += count;
            flushBlock();
        }
        traits.writeValues(getProxyValueWriter(), values, curoff, curlen);
        super.curTuple += curlen;
    }
    @Override
    public void writeValue(T value) throws IOException {
        flushBlockIfNeeded();
        traits.writeValue(getProxyValueWriter(), value);
        ++super.curTuple;
    }
}
