package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * File writer for variable-length values.
 * 
 * Because each value is variable-length, the resulted file
 * does not allow seeking to arbitrary tuple position by itself.
 * Thus, this writer also collects tuple positions
 * to produce a "position file" as a sparse index.
 */
public class LocalVarLenWriter<T> extends LocalRawFileWriter {
    private final VarLenValueTraits<T> traits;

    private final int collectPerBytes;
    private long prevCollectPosition = Long.MIN_VALUE; // to always collect at the first value
    private long curTuple = 0L;
    private ArrayList<Long> collectedTuples = new ArrayList<Long>();
    private ArrayList<Long> collectedPositions = new ArrayList<Long>();
    public LocalVarLenWriter(File file, VarLenValueTraits<T> traits, int collectPerBytes) throws IOException {
        super (file, 1 << 18);
        this.traits = traits;
        this.collectPerBytes = collectPerBytes;
    }
    
    public void writeValue (T value) throws IOException {
        collectTuplePosition();
        byte[] bytes = traits.toBytes(value);
        int totalBytes; // including length header
        if (bytes.length < (1 << 7)) {
            // 1 byte length header
            writeByte((byte) 1);
            writeByte((byte) bytes.length);
            totalBytes = 1 + 1 + bytes.length;
        } else if (bytes.length < (1 << 15)) {
            // 2 byte length header
            writeByte((byte) 2);
            totalBytes = 1 + 2 + bytes.length;
            writeShort((short) bytes.length);
        } else if (bytes.length < (1 << 31)) {
            // 4 byte length header
            writeByte((byte) 4);
            totalBytes = 1 + 4 + bytes.length;
            writeInt(bytes.length);
        } else {
            // 8 byte length header (this is not quite implemented as byte[1<<32] isn't possible)
            writeByte((byte) 8);
            totalBytes = 1 + 8 + bytes.length;
            writeLong(bytes.length);
        }
        writeBytes(bytes, 0, bytes.length);
        ++curTuple;
        curPosition += totalBytes;
    }

    private void collectTuplePosition () {
        if (curPosition - prevCollectPosition >= collectPerBytes) {
            collectedTuples.add(curTuple);
            collectedPositions.add(curPosition);
            prevCollectPosition = curPosition;
            assert (collectedTuples.size() == collectedPositions.size());
        }
    }

    /**
     * Writes out the collected positions to a position file.
     */
    public void writePositionFile (File posFile) throws IOException {
        LocalPosFile.createPosFile(posFile, collectedTuples, collectedPositions);
    }
}
