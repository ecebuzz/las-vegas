package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueIndex;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Implementation of value index file.
 * @see ValueIndex
 * @see LocalPosFile
 */
public class LocalValFile<T extends Comparable<T>, AT> implements ValueIndex<T> {
    private static Logger LOG = Logger.getLogger(LocalValFile.class);

    /**
     * Reads a value index file into this object.
     * This object simply reads the entire file like
     * {@link LocalPosFile#LocalPosFile(VirtualFile)}.
     * @param file the value index file
     * @param type value type of the data file (could be different from column type itself if dictionary-compressed)
     * @see #writeToFile(VirtualFile)
     */
    @SuppressWarnings("unchecked")
    public LocalValFile (VirtualFile file, ColumnType type) throws IOException {
        this.traits = (ValueTraits<T, AT>) AllValueTraits.getInstance(type); 
        int fileSize = (int) file.length();
        if (fileSize > 1 << 24) {
            throw new IOException ("the value index file : " + file + " seems too large. " + (fileSize >> 20) + "MB");
        }
        byte[] bytes = new byte[fileSize];
        InputStream in = file.getInputStream();
        int read = in.read(bytes);
        in.close();
        assert (read == bytes.length);
        ByteBuffer byteBuffer  = ByteBuffer.wrap(bytes);
        this.tuplePosArray = intTraits.deserializeArray(byteBuffer);
        this.valueArray = traits.deserializeArray(byteBuffer);
        this.valueCount = tuplePosArray.length;
        assert (valueCount == traits.length(valueArray));
        assert (byteBuffer.position() == bytes.length);
        assert (validateValueArray());
    }

    /**
     * Constructs a value index with the given values.
     * Used when writing out a value index file.
     */
    @SuppressWarnings("unchecked")
    public LocalValFile (List<T> values, List<Integer> tuplePositions, ColumnType type) {
        this.traits = (ValueTraits<T, AT>) AllValueTraits.getInstance(type);
        assert (values.size() == tuplePositions.size());
        this.tuplePosArray = intTraits.toArray(tuplePositions);
        this.valueCount = values.size();
        this.valueArray = traits.toArray(values);
        assert (validateValueArray());
    }
    
    /** used for assertion. not supposed to be fast. */
    private boolean validateValueArray() {
        if (LOG.isDebugEnabled()) {
            T prev = traits.get(valueArray, 0);
            assert (tuplePosArray[0] == 0);
            for (int i = 1; i < valueCount; ++i) {
                T cur = traits.get(valueArray, i);
                if (prev.compareTo(cur) > 0) {
                    return false;
                }
                prev = cur;
                assert (tuplePosArray[i] > tuplePosArray[i - 1]);
            }
        }
        return true;
    }
    
    private final int[] tuplePosArray;
    private final AT valueArray;
    private final int valueCount;
    private final ValueTraits<T, AT> traits;
    private final AllValueTraits.IntegerValueTraits intTraits = new AllValueTraits.IntegerValueTraits();
    
    @Override
    public int searchValues(T value) {
        // find the position to start from.
        // Here, the value array stores the values at each periodically collected tuple position.
        // It does NOT guarantee that the tuple is the first tuple to have the value due to ties.
        // So, we need to return the position where the value is strictly less than the searched key.
        int pos = traits.binarySearch(valueArray, value);
        if (pos >= 0) {
            // exact match. for the reason above, we need to start from previous entry.
            // go back until we find a strictly smaller entry (remember, Arrays#binarySearch doesn't guarantee anything about ties)
            for (; pos >= 0 ; --pos) {
                T prev = traits.get(valueArray, pos);
                if (prev.compareTo(value) < 0) {
                    break;
                }
            }
        } else  {
            // then, Arrays#binarySearch returned (-insertion point - 1).
            // because the one before the insertion point is strictly smaller, we just return it.
            pos = - pos - 1 - 1; // -1 to cancel the -1. another -1 to get the one before
        }

        if (pos < 0) {
            // even if the first entry is not strictly smaller, we can safely start from there because it's the first tuple!
            if (traits.get(valueArray, 0).equals(value)) {
                return 0;
            }
            // if even the first tuple is larger than the key, this partition doesn't contain the key.
            return -1;
        }
        return tuplePosArray[pos];
    }
    
    @Override
    public void writeToFile(VirtualFile file) throws IOException {
        long startMillisec = System.currentTimeMillis();
        int fileSize = intTraits.getSerializedByteSize(tuplePosArray) + traits.getSerializedByteSize(valueArray);
        if (fileSize > 1 << 26) {
            throw new IOException ("This value index will be too large: " + (fileSize >> 20) + "MB");
        }
        byte[] bytes = new byte[fileSize];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int writtenBytes = 0;
        writtenBytes += intTraits.serializeArray(tuplePosArray, byteBuffer);
        assert (byteBuffer.position() == writtenBytes);
        writtenBytes += traits.serializeArray(valueArray, byteBuffer);
        assert (bytes.length == writtenBytes);
        assert (byteBuffer.position() == writtenBytes);
        OutputStream out = file.getOutputStream();
        out.write(bytes);
        out.flush();
        out.close();
        long endMillisec = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("Wrote out a value index file (" + file.getAbsolutePath()
                            + "):" + valueCount + " entries, " + file.length()
                            + " bytes, in " + (endMillisec - startMillisec) + "ms");
        }
    }
}
