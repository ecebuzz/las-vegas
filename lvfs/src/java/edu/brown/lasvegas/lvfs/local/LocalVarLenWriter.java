package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VarLenValueTraits;

/**
 * File writer for variable-length values.
 * 
 * Because each value is variable-length, the resulted file
 * does not allow seeking to arbitrary tuple position by itself.
 * Thus, this writer also collects tuple positions
 * to produce a "position file" as a sparse index.
 */
public final class LocalVarLenWriter<T> extends LocalRawFileWriter implements TypedWriter<T, T[]>  {
    /** Constructs an instance of varchar column. */
    public static LocalVarLenWriter<String> getInstanceVarchar(File rawFile, int collectPerBytes) throws IOException {
        return new LocalVarLenWriter<String>(rawFile, new AllValueTraits.VarcharValueTraits(), collectPerBytes);
    }
    /** Constructs an instance of varbinary column. */
    public static LocalVarLenWriter<byte[]> getInstanceVarbin(File rawFile, int collectPerBytes) throws IOException {
        return new LocalVarLenWriter<byte[]>(rawFile, new AllValueTraits.VarbinValueTraits(), collectPerBytes);
    }

    private final VarLenValueTraits<T> traits;

    private final int collectPerBytes;
    private long prevCollectPosition = -1L; // to always collect at the first value
    private long curTuple = 0L;
    private ArrayList<Long> collectedTuples = new ArrayList<Long>();
    private ArrayList<Long> collectedPositions = new ArrayList<Long>();
    public LocalVarLenWriter(File file, VarLenValueTraits<T> traits, int collectPerBytes) throws IOException {
        super (file, 1 << 18);
        this.traits = traits;
        this.collectPerBytes = collectPerBytes;
    }
    @Override
    public void writeValues(T[] values, int off, int len) throws IOException {
        // simply loop.
        // because of length header, there is no faster way to do this.
        for (int i = off; i < off + len; ++i) {
            writeValue(values[i]);
        }
    }
    @Override
    public void writeValue (T value) throws IOException {
        collectTuplePosition();
        traits.writeValue(getRawValueWriter(), value);
        ++curTuple;
    }

    private void collectTuplePosition () {
        if (prevCollectPosition < 0 || getCurPosition() - prevCollectPosition >= collectPerBytes) {
            collectedTuples.add(curTuple);
            collectedPositions.add(getCurPosition());
            prevCollectPosition = getCurPosition();
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
