package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;

/**
 * Reader implementation of block-compressed files for fixed-length columns.
 * As this is fixed-length, each block is simply an array of values.
 * No footer at the end of block.
 * @param <T> Value type (e.g., Integer)
 * @param <AT> Array type (e.g., int[]).
 */
public final class LocalBlockCompressionFixLenReader<T, AT> extends LocalBlockCompressionReader<T, AT> {
    private final FixLenValueTraits<T, AT> traits;
    private final short bitsPerValue;
    
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenReader<Byte, byte[]> getInstanceTinyint(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenReader<Byte, byte[]>(file, new AllValueTraits.TinyintValueTraits(), compressionType);
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenReader<Short, short[]> getInstanceSmallint(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenReader<Short, short[]>(file, new AllValueTraits.SmallintValueTraits(), compressionType);
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenReader<Integer, int[]> getInstanceInteger(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenReader<Integer, int[]>(file, new AllValueTraits.IntegerValueTraits(), compressionType);
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalBlockCompressionFixLenReader<Long, long[]> getInstanceBigint(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenReader<Long, long[]>(file, new AllValueTraits.BigintValueTraits(), compressionType);
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalBlockCompressionFixLenReader<Float, float[]> getInstanceFloat(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenReader<Float, float[]>(file, new AllValueTraits.FloatValueTraits(), compressionType);
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalBlockCompressionFixLenReader<Double, double[]> getInstanceDouble(File file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionFixLenReader<Double, double[]>(file, new AllValueTraits.DoubleValueTraits(), compressionType);
    }

    public LocalBlockCompressionFixLenReader(File file, FixLenValueTraits<T, AT> traits, CompressionType compressionType) throws IOException {
        super (file, traits, compressionType);
        this.traits = traits;
        this.bitsPerValue = traits.getBitsPerValue();
    }
    
    @Override
    public int readValues (AT values, int off, int len) throws IOException {
        if (currentBlockIndex < 0) {
            seekToBlock(0);
        }
        int totalRead = 0;
        while (totalRead < len) {
            if (!getProxyValueReader().hasMore()) {
                return totalRead; //EOF
            }
            if (currentBlockTuple >= blockTupleCounts[currentBlockIndex]) {
                seekToBlock(currentBlockIndex + 1);
                assert (currentBlockTuple == 0);
                assert (currentBlockCursor == 0);
            }
            int nextRead = Math.min(blockTupleCounts[currentBlockIndex] - currentBlockTuple, len);
            int read = traits.readValues(getProxyValueReader(), values, off + totalRead, nextRead);
            assert (read == nextRead);
            totalRead += read;
            currentBlockTuple += read;
        }
        return totalRead;
    }
    @Override
    public T readValue() throws IOException {
        if (currentBlockIndex < 0) {
            seekToBlock(0);
        }
        if (currentBlockTuple + 1 >= blockTupleCounts[currentBlockIndex]) {
            // move to next block
            seekToBlock(currentBlockIndex + 1);
            assert (currentBlockTuple == 0);
        }
        T value = traits.readValue(getProxyValueReader());
        ++currentBlockTuple;
        return value;
    }
    
    @Override
    public void skipValue() throws IOException {
        skipValues(1);
    }
    @Override
    public void skipValues(int skip) throws IOException {
        seekToTupleRelative(skip);
    }

    /**
     * Jump to the specified absolute tuple position.
     */
    public void seekToTupleAbsolute (int tuple) throws IOException {
        if (tuple < 0 || tuple >= totalTuples) {
            throw new IOException ("invalid tuple position specified:" + tuple);
        }
        if (currentBlockIndex < 0
            || blockStartTuples[currentBlockIndex] > tuple
            || blockStartTuples[currentBlockIndex] + blockTupleCounts[currentBlockIndex] <= tuple) {
            // we have to move to other block 
            int block = searchBlock(tuple);
            seekToBlock(block);
            int blockTuple = tuple - blockStartTuples[currentBlockIndex];
            assert (blockTuple >= 0);
            getProxyValueReader().skipBytes(blockTuple * bitsPerValue / 8);
            currentBlockTuple = blockTuple;
        } else {
            // we are in the desired block
            int blockTuple = tuple - blockStartTuples[currentBlockIndex];
            assert (blockTuple > 0);
            if (blockTuple >= currentBlockTuple) {
                getProxyValueReader().skipBytes((blockTuple - currentBlockTuple) * bitsPerValue / 8);
            } else {
                seekToBlock(currentBlockIndex); // this resets in-block cursor
                getProxyValueReader().skipBytes(blockTuple * bitsPerValue / 8);
            }
            currentBlockTuple = blockTuple;
        }
    }

    /**
     * Jump to the specified tuple position relative to current position.
     */
    public void seekToTupleRelative (int tuple) throws IOException {
        if (currentBlockIndex < 0) {
            seekToTupleAbsolute(tuple);
        } else {
            seekToTupleAbsolute(blockStartTuples[currentBlockIndex] + currentBlockTuple + tuple);
        }
    }

    // fix-length doesn't need block footer
    @Override
    protected void readBlockFooter() throws IOException {
    }
    @Override
    protected int getCurrentBlockFooterByteSize() {
        return 0;
    }
}
