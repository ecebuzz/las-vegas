package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VarLenValueTraits;

/**
 * Implementation of block-compressed files for variable-length columns.
 * <p>As this is variable-length, seeking in each block is not trivial.
 * Thus, each block ends with a list of position indexes just like
 * a position file for variable-length file. However, as this is 
 * just for a block, both tuple numbers and byte positions are 4 byte integers
 * and relative to the beginning of the block.</p>
 * 
 * <p>At the end of a block, a 4-byte integer specifies how many positions
 * are stored as footer (just like position file, this is a sparse index).
 * Let the number be n. 2n 4-byte integers precede the end-of-block
 * value. These integers are pairs of the tuple number
 * <b>RELATIVE TO the block's beginning tuple number</b> and the byte position
 * <b>IN THE BLOCK after decompression</b>.</p>
 * @param <T> Value type (e.g., String)
 */
public final class LocalBlockCompressionVarLenWriter<T> extends LocalBlockCompressionWriter implements TypedWriter<T, T[]> {
    private final VarLenValueTraits<T> traits;
    private final int collectPerBytes;

    // note that these are relative to the beginning of current block 
    private int prevCollectPosition = -1; // to always collect at the first value
    private int relativeTuple = 0;
    private ArrayList<Integer> collectedTuples = new ArrayList<Integer>();
    private ArrayList<Integer> collectedPositions = new ArrayList<Integer>();

    /** Constructs an instance of varchar column. */
    public static LocalBlockCompressionVarLenWriter<String> getInstanceVarchar(
                    File file, CompressionType compressionType, int collectPerBytes) throws IOException {
        return new LocalBlockCompressionVarLenWriter<String>(file, compressionType, new AllValueTraits.VarcharValueTraits(), collectPerBytes);
    }
    /** Constructs an instance of varbinary column. */
    public static LocalBlockCompressionVarLenWriter<byte[]> getInstanceVarbin(
                    File file, CompressionType compressionType, int collectPerBytes) throws IOException {
        return new LocalBlockCompressionVarLenWriter<byte[]>(file, compressionType, new AllValueTraits.VarbinValueTraits(), collectPerBytes);
    }

    public LocalBlockCompressionVarLenWriter(File file, CompressionType compressionType, VarLenValueTraits<T> traits, int collectPerBytes) throws IOException {
        super (file, compressionType);
        this.traits = traits;
        this.collectPerBytes = collectPerBytes;
    }

    @Override
    public void writeValues (T[] values, int off, int len) throws IOException {
        for (int i = off; i < off + len; ++i) {
            writeValue(values[i]);
        }
    }
    @Override
    public void writeValue(T value) throws IOException {
        flushBlockIfNeeded();
        collectTuplePosition();
        traits.writeValue(getProxyValueWriter(), value);
        ++super.curTuple;
        ++relativeTuple;
    }
    private void collectTuplePosition () {
        if (prevCollectPosition < 0 || currentBlockUsed - prevCollectPosition >= collectPerBytes) {
            collectedTuples.add(relativeTuple);
            collectedPositions.add(currentBlockUsed);
            prevCollectPosition = currentBlockUsed;
            assert (collectedTuples.size() == collectedPositions.size());
        }
    }
    @Override
    protected void appendBlockFooter() throws IOException {
        final int count = collectedTuples.size();
        int[] intBuf = new int[count * 2 + 1];
        for (int i = 0; i < count; ++i) {
            intBuf[i * 2] = collectedTuples.get(i);
            intBuf[i * 2 + 1] = collectedPositions.get(i);
        }
        intBuf[intBuf.length - 1] = count;
        byte[] buf = new byte[4 * intBuf.length];
        ByteBuffer.wrap(buf).asIntBuffer().put(intBuf);


        // clear objects about current blocks
        prevCollectPosition = -1;
        relativeTuple = 0;
        collectedTuples.clear();
        collectedPositions.clear();
    }
}
