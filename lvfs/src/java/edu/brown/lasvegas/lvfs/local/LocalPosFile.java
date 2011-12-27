package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.PositionIndex;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

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
 * <p>Writing is simply to dump the bunch of int-values. Reading
 * is a binary-search on the tuple-num.</p>
 */
public final class LocalPosFile implements PositionIndex {
    private static Logger LOG = Logger.getLogger(LocalPosFile.class);
    
    /**
     * Newly creates a position file with the given tuple-num and byte positions.
     * @param file the file to write.
     * @param tuples The tuple numbers (in the data file alone, so always starts with 0)
     * @param positions byte positions of the tuples.
     * @param totalTuples total number of tuples in the file
     * @param totalBytes total byte size of the file
     * @throws IOException
     */
    public static void createPosFile (VirtualFile file, ArrayList<Integer> tuples, ArrayList<Integer> positions,
                    int totalTuples, int totalBytes) throws IOException {
        assert (tuples.size() == positions.size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("writing " + tuples.size() + " positions into a position file " + file.getAbsolutePath());
        }
        long startTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L;
        int[] array = new int[(tuples.size() + 1) * 2];
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
        // last entry always gives the total number of tuples and end of file position 
        array[tuples.size() * 2] = totalTuples;
        array[tuples.size() * 2 + 1] = totalBytes;
        long midTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L; // after CPU intensive stuffs
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert-time=" + (midTime - startTime) + "ns");
        }
        writeToFile (file, array);
    }
    
    /** content of this file. see the class comment for the file format. */
    private final int[] array;

    /**
     * Reads a position file into this object.
     * This object simply reads the entire file even though we might actually need
     * a part of the file. However, position file should be small and other costs
     * such as disk-seek and reading the data file should be the dominant cost.
     * Adding complexity wouldn't worth it.
     */
    public LocalPosFile(VirtualFile file) throws IOException {
        assert (file.length() % 8 == 0);
        if (file.length() > (1 << 22)) {
            throw new IOException ("this file seems too large as a position file:"
                + file.getAbsolutePath()
                + "=" + file.length() + " bytes (" + (file.length()<<20) + "MB)");
        }
        long startTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L;
        array = new int[(int) file.length() / 4];
        byte[] bytes = new byte[(int) file.length()];
        InputStream stream = file.getInputStream();
        int read = stream.read(bytes);
        stream.close();
        assert (read == bytes.length);
        // simply reads it to long[]. we don't convert it to Pos[] because
        // it will create a lot of unused objects.
        ByteBuffer.wrap(bytes).asIntBuffer().get(array);
        long endTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L;
        if (LOG.isDebugEnabled()) {
            LOG.debug("read " + array.length + " long values from a position file " + file.getAbsolutePath()
                    + ". time=" + (endTime - startTime) + "ns");
        }
    }

    @Override
    public Pos searchPosition (int tupleToFind) {
        if (tupleToFind < 0 || tupleToFind  >= getTotalTuples()) { 
            throw new IllegalArgumentException("this tuple position does not exist in this file:" + tupleToFind);
        }
        assert (array[0] == 0);
        // first, assure the first entry is strictly smaller than the searched tuple
        if (tupleToFind == 0) {
            return new Pos (array[0], array[1]);
        }
        // the last entry is always strictly larger than the searched tuple
        
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
    
    @Override
    public int getTotalTuples () {
        // the last tuple must point to the end of the file (non existing tuple)
        return (int) array[array.length - 2];
    }

    @Override
    public int getTotalBytes () {
        return array[array.length - 1];
    }
    
    @Override
    public void writeToFile(VirtualFile file) throws IOException {
        writeToFile (file, array);
    }
    private static void writeToFile (VirtualFile file, int[] theArray) throws IOException {
        byte[] bytes = new byte[theArray.length * 4];
        ByteBuffer.wrap(bytes).asIntBuffer().put(theArray);
        long midTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L; // after CPU intensive stuffs
        VirtualFileOutputStream stream = file.getOutputStream();
        stream.write(bytes);
        stream.flush();
        stream.close();
        long endTime = LOG.isDebugEnabled() ? System.nanoTime() : 0L; // after all
        if (LOG.isDebugEnabled()) {
            LOG.debug("wrote " + theArray.length + " integers into a position file " + file.getAbsolutePath()
                    + ". IO-time=" + (endTime - midTime) + "ns");
        }
    }
}
