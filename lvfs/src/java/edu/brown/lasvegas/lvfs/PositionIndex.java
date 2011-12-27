package edu.brown.lasvegas.lvfs;

import java.io.IOException;

/**
 * A position file is a sparse tuple-position index file for variable-length values
 * and a few non-seekable compressions (e.g., RLE, Snappy).
 * 
 * <p>A position file simply stores lots of long values so that both reading and
 * writing are extremely simple and fast. Also, a position file should be small,
 * most likely 100-200KB or so. So, we read/write them at once.</p>
 * 
 * <p>The file format is a series of long-pairs.
 * 1) tuple-num of the pointed tuple.
 * 2) byte position of the tuple.</p>
 * 
 * <p>Additionally, it's guaranteed that the first pair points to the first tuple
 * in the file, the last pair points to the last tuple + 1 (=the total number of tuples).</p>
 * 
 * <p>Writing is simply to dump the bunch of long-values. Reading
 * is a binary-search on the tuple-num.</p>
 */
public interface PositionIndex {
    /**
     * Tuple-number and byte-position pair.
     */
    public static class Pos {
        public Pos (int tuple, int bytePosition) {
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
     * Returns the best position to <b>start</b> reading the file.
     * The returned position is probably not the searched tuple itself
     * because a position file is a sparse index file.
     * However, starting from the returned position will always find
     * the searched tuple.
     * @param tupleToFind the tuple position to find.
     * @return the best position to <b>start</b> reading the file.
     */
    public Pos searchPosition (int tupleToFind);
    
    /**
     * Returns the total number of tuples in the data file.
     */
    public int getTotalTuples ();

    /**
     * Returns the total byte size of the data file.
     */
    public int getTotalBytes ();


    /**
     * Writes out the position file.
     */
    public void writeToFile (VirtualFile file) throws IOException;
}
