package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.VarLenValueTraits;
import edu.brown.lasvegas.traits.VarbinValueTraits;
import edu.brown.lasvegas.traits.VarcharValueTraits;
import edu.brown.lasvegas.util.ByteArray;

/**
 * Reader implementation of block-compressed files for variable-length columns.
 * @param <T> Value type (e.g., String)
 */
public class LocalBlockCompressionVarLenReader<T extends Comparable<T>> extends LocalBlockCompressionReader<T, T[]> {
    private final VarLenValueTraits<T> traits;
    /** Variable-length block has position indexes. See class comments of {@link LocalBlockCompressionVarLenWriter}. */
    private int[] currentBlockFooter;
    
    /** Constructs an instance of varchar column. */
    public static LocalBlockCompressionVarLenReader<String> getInstanceVarchar(
                    VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionVarLenReader<String>(file, new VarcharValueTraits(), compressionType);
    }
    /** Constructs an instance of varbinary column. */
    public static LocalBlockCompressionVarLenReader<ByteArray> getInstanceVarbin(
                    VirtualFile file, CompressionType compressionType) throws IOException {
        return new LocalBlockCompressionVarLenReader<ByteArray>(file, new VarbinValueTraits(), compressionType);
    }

    public LocalBlockCompressionVarLenReader(VirtualFile file, VarLenValueTraits<T> traits, CompressionType compressionType) throws IOException {
        super (file, traits, compressionType);
        this.traits = traits;
    }
    
    @Override
    public T readValue() throws IOException {
        if (currentBlockIndex < 0) {
            seekToBlock(0);
        }
        if (!getProxyValueReader().hasMore()) {
            throw new IOException("EOF");
        }
        if (currentBlockTuple >= blockTupleCounts[currentBlockIndex]) {
            // move to next block
            seekToBlock(currentBlockIndex + 1);
            assert (currentBlockTuple == 0);
            assert (currentBlockCursor == 0);
        }
        T value = traits.readValue(getProxyValueReader());
        ++currentBlockTuple;
        return value;
    }
    @Override
    public int readValues(T[] buffer, int off, int len) throws IOException {
        for (int i = off; i < off + len; ++i) {
            if (!getProxyValueReader().hasMore()) {
                return i - off; // EOF
            }
            buffer[i] = readValue();
        }
        return len;
    }
    @Override
    public void skipValue() throws IOException {
        skipValues(1);
    }
    @Override
    public void skipValues(int skip) throws IOException {
        assert (skip > 0);
        if (currentBlockIndex < 0) {
            seekToBlock(0);
        }
        if (currentBlockTuple + skip >= blockTupleCounts[currentBlockIndex]) {
            // we have to move to other block 
            int tupleToFind = blockStartTuples[currentBlockIndex] + currentBlockTuple + skip;
            int block = searchBlock(tupleToFind);
            seekToBlock(block);
            assert (currentBlockTuple == 0);
            int toSkip = tupleToFind - blockStartTuples[currentBlockIndex];
            if (toSkip > 0) {
                skipInBlock (toSkip);
            }
        } else {
            // we are in the desired block
            skipInBlock (skip);
        }
    }
    @Override
    public void seekToTupleAbsolute(int tuple) throws IOException {
        // need to move to other block?
        if (currentBlockIndex < 0 || tuple < blockStartTuples[currentBlockIndex]
          || tuple >= blockStartTuples[currentBlockIndex] + blockTupleCounts[currentBlockIndex]) {
            int block = searchBlock(tuple);
            seekToBlock(block);
            assert (currentBlockCursor == 0);
            assert (currentBlockTuple == 0);
        }

        // need to move back to the beginning of this block?
        if (blockStartTuples[currentBlockIndex] + currentBlockTuple > tuple) {
            currentBlockCursor = 0;
            currentBlockTuple = 0;
        }

        // then just skip in the blcok, which might internally do binary search
        skipInBlock(tuple - blockStartTuples[currentBlockIndex] - currentBlockTuple);
    }
    
    /** skip specified number of values within this block. */
    private void skipInBlock (int skip) throws IOException {
        if (skip == 0) {
            return;
        }
        assert (currentBlockIndex >= 0);
        assert (currentBlockTuple + skip < blockTupleCounts[currentBlockIndex]);
        if (skip < 100) {
            // don't consider using position index for small skips
            skipInBlockSequentially (skip);
        } else {
            // do binary search
            int tupleToFind = currentBlockTuple + skip;
            InBlockPos pos = searchInBlockPosition(tupleToFind);
            currentBlockCursor = pos.bytePosition;
            currentBlockTuple = pos.tuple;
            // remaining is sequential search
            skipInBlockSequentially (tupleToFind - pos.tuple);
        }
    }
    private void skipInBlockSequentially (int skip) throws IOException {
        ProxyValueReader reader = getProxyValueReader();
        for (int i = 0; i < skip; ++i) {
            int bytesToSkip = reader.readLengthHeader();
            assert (bytesToSkip >= 0);
            reader.skipBytes(bytesToSkip);
        }
        currentBlockTuple += skip;
    }
    
    /**
     * Tuple-number and byte-position pair.
     */
    private static class InBlockPos {
        InBlockPos (int tuple, int bytePosition) {
            this.tuple = tuple;
            this.bytePosition = bytePosition;
        }
        public final int tuple;
        public final int bytePosition;
        @Override
        public String toString() {
            return "Pos: tuple=" + tuple + ", from " + bytePosition + "th bytes";
        }
    }
    /**
     * Returns the best position to <b>start</b> reading the block.
     * The returned position is probably not the searched tuple itself
     * because a position index is a sparse index.
     * However, starting from the returned position will always find
     * the searched tuple.
     * @param tupleToFind the tuple position to find.
     * @return the best position to <b>start</b> reading the block.
     */
    private InBlockPos searchInBlockPosition (int tupleToFind) {
        // first, assure the first entry is strictly smaller than the searched tuple
        int[] array = currentBlockFooter;
        if (array[0] >= tupleToFind) {
            return new InBlockPos (array[0], array[1]);
        }
        // also, assure the last entry is strictly larger than the searched tuple
        if (array[array.length - 2] <= tupleToFind) {
            return new InBlockPos (array[array.length - 2], array[array.length - 1]);
        }
        
        // then, binary search
        int low = 0; // the entry we know strictly smaller
        int high = (array.length / 2); // the entry we know strictly larger
        int mid = 0;
        while (low <= high) {
            mid = (low + high) >>> 1;
            int midTuple = array[mid * 2];
            if (midTuple < tupleToFind) {
                low = mid + 1;
            } else if (midTuple > tupleToFind) {
                high = mid - 1;
            } else {
                return new InBlockPos (midTuple, array[mid * 2 + 1]); // exact match. lucky!
            }
        }
        // not exact match. in this case, return the position we should start searching
        int ret = (low > mid) ? low - 1 : mid - 1;
        assert (ret >= 0);
        assert (ret < array.length / 2);
        assert (array[ret * 2] < tupleToFind);
        assert (ret == (array.length / 2) - 1 || array[(ret + 1) * 2] > tupleToFind);
        return new InBlockPos (array[ret * 2], array[ret * 2 + 1]);
    }
    
    @Override
    protected void readBlockFooter() throws IOException {
        currentBlockCursor = currentBlock.length - 4;
        int positionCount = getProxyValueReader().readInt();
        assert (positionCount > 0);
        currentBlockFooter = new int[2 * positionCount];
        currentBlockCursor = currentBlock.length - 4 - 4 * 2 * positionCount;
        int intRead = getProxyValueReader().readInts(currentBlockFooter, 0, currentBlockFooter.length);
        assert (intRead == currentBlockFooter.length);
        currentBlockCursor = 0;
    }
    @Override
    protected int getCurrentBlockFooterByteSize() {
        return currentBlockFooter.length * 4 + 4;
    }
}
