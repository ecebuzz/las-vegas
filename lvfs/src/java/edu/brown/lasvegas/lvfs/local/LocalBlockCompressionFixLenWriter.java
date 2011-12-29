package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Writer implementation of block-compressed files for fixed-length columns.
 * As this is fixed-length, each block is simply an array of values.
 * No footer at the end of block.
 * @param <T> Value type (e.g., Integer)
 * @param <AT> Array type (e.g., int[]).
 */
public final class LocalBlockCompressionFixLenWriter<T extends Comparable<T>, AT> extends LocalBlockCompressionWriter<T, AT> {
    private final FixLenValueTraits<T, AT> traits;
    
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Byte, byte[]> getInstanceTinyint(VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Byte, byte[]>(file, new AllValueTraits.TinyintValueTraits(), compressionType);
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Short, short[]> getInstanceSmallint(VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Short, short[]>(file, new AllValueTraits.SmallintValueTraits(), compressionType);
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Integer, int[]> getInstanceInteger(VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Integer, int[]>(file, new AllValueTraits.IntegerValueTraits(), compressionType);
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenWriter<Long, long[]> getInstanceBigint(VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Long, long[]>(file, new AllValueTraits.BigintValueTraits(), compressionType);
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalBlockCompressionFixLenWriter<Float, float[]> getInstanceFloat(VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Float, float[]>(file, new AllValueTraits.FloatValueTraits(), compressionType);
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalBlockCompressionFixLenWriter<Double, double[]> getInstanceDouble(VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenWriter<Double, double[]>(file, new AllValueTraits.DoubleValueTraits(), compressionType);
    }

    public LocalBlockCompressionFixLenWriter(VirtualFile file, FixLenValueTraits<T, AT> traits, CompressionType compressionType) throws IOException {
        super (file, traits, compressionType);
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
