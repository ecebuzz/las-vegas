package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

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
 * <p>Writing is simply to dump the bunch of long-values. Reading
 * is a binary-search on the tuple-num.</p>
 */
public final class LocalPosFile {
    private static Logger LOG = Logger.getLogger(LocalPosFile.class);
    
    /**
     * Newly creates a position file with the given tuple-num and byte positions.
     * @param file the file to write.
     * @param tuples The tuple numbers (in the data file alone, so always starts with 0)
     * @param positions byte positions of the tuples.
     * @throws IOException
     */
    public static void createPosFile (File file, ArrayList<Long> tuples, ArrayList<Long> positions) throws IOException {
        assert (tuples.size() == positions.size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("writing " + tuples.size() + " positions into a position file " + file.getAbsolutePath());
        }
        long startTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L;
        long[] array = new long[tuples.size() * 2];
        for (int i = 0; i < tuples.size(); ++i) {
            array[i * 2] = tuples.get(i);
            array[i * 2 + 1] = positions.get(i);
            if (i == 0) {
                assert (array[i * 2] == 0);
            } else {
                assert (array[i * 2] > array[(i - 1) * 2]); 
                assert (array[i * 2 + 1] > array[(i - 1) * 2 + 1]); 
            }
        }
        byte[] bytes = new byte[array.length * 8];
        ByteBuffer.wrap(bytes).asLongBuffer().put(array);
        long midTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L; // after CPU intensive stuffs
        if (file.exists()) {
            file.delete();
        }
        FileOutputStream stream = new FileOutputStream(file, false);
        stream.write(bytes);
        stream.flush();
        stream.close();
        long endTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L; // after all
        if (LOG.isDebugEnabled()) {
            LOG.debug("wrote " + tuples.size() + " positions into a position file " + file.getAbsolutePath()
                    + ". convert-time=" + (midTime - startTime) + "ns, IO-time=" + (endTime - midTime) + "ns");
        }
    }
    
    /** content of this file. see the class comment for the file format. */
    private final long[] array;

    /**
     * Reads a position file into this object.
     * This object simply reads the entire file even though we might actually need
     * a part of the file. However, position file should be small and other costs
     * such as disk-seek and reading the data file should be the dominant cost.
     * Adding complexity wouldn't worth it.
     */
    public LocalPosFile(File file) throws IOException {
        assert (file.length() % 16 == 0);
        if (file.length() > (1 << 22)) {
            throw new IOException ("this file seems too large as a position file:"
                + file.getAbsolutePath()
                + "=" + file.length() + " bytes (" + (file.length()<<20) + "MB)");
        }
        long startTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L;
        array = new long[(int) file.length() / 8];
        byte[] bytes = new byte[(int) file.length()];
        FileInputStream stream = new FileInputStream(file);
        int read = stream.read(bytes);
        stream.close();
        assert (read == bytes.length);
        // simply reads it to long[]. we don't convert it to Pos[] because
        // it will create a lot of unused objects.
        ByteBuffer.wrap(bytes).asLongBuffer().get(array);
        long endTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L;
        if (LOG.isDebugEnabled()) {
            LOG.debug("read " + array.length + " long values from a position file " + file.getAbsolutePath()
                    + ". time=" + (endTime - startTime) + "ns");
        }
    }
    
    /**
     * Tuple-number and byte-position pair.
     */
    public static class Pos {
        Pos (long tuple, long bytePosition) {
            this.tuple = tuple;
            this.bytePosition = bytePosition;
        }
        public final long tuple;
        public final long bytePosition;
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
    public Pos searchPosition (long tupleToFind) {
        // first, assure the first entry is strictly smaller than the searched tuple
        if (array[0] >= tupleToFind) {
            return new Pos (array[0], array[1]);
        }
        // also, assure the last entry is strictly larger than the searched tuple
        if (array[array.length - 2] <= tupleToFind) {
            return new Pos (array[array.length - 2], array[array.length - 1]);
        }
        
        // then, binary search
        int low = 0; // the entry we know strictly smaller
        int high = (array.length / 2); // the entry we know strictly larger
        int mid = 0;
        while (low <= high) {
            mid = (low + high) >>> 1;
            long midTuple = array[mid * 2];
            if (midTuple < tupleToFind) {
                low = mid + 1;
            } else if (midTuple > tupleToFind) {
                high = mid - 1;
            } else {
                return new Pos (midTuple, array[mid * 2 + 1]); // exact match. lucky!
            }
        }
        // not exact match. in this case, return the position we should start searching
        int ret = (low > mid) ? low - 1 : mid - 1;
        assert (ret >= 0);
        assert (ret < array.length / 2);
        assert (array[ret * 2] < tupleToFind);
        assert (ret == (array.length / 2) - 1 || array[(ret + 1) * 2] > tupleToFind);
        return new Pos (array[ret * 2], array[ret * 2 + 1]);
    }
}
