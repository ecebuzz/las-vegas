package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.util.ArrayList;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.VarLenValueTraits;
import edu.brown.lasvegas.traits.VarbinValueTraits;
import edu.brown.lasvegas.traits.VarcharValueTraits;
import edu.brown.lasvegas.util.ByteArray;

/**
 * File writer for variable-length values.
 * 
 * Because each value is variable-length, the resulted file
 * does not allow seeking to arbitrary tuple position by itself.
 * Thus, this writer also collects tuple positions
 * to produce a "position file" as a sparse index.
 */
public final class LocalVarLenWriter<T extends Comparable<T>> extends LocalTypedWriterBase<T, T[]>  {
    /** Constructs an instance of varchar column. */
    public static LocalVarLenWriter<String> getInstanceVarchar(VirtualFile rawFile, int collectPerBytes) throws IOException {
        return new LocalVarLenWriter<String>(rawFile, new VarcharValueTraits(), collectPerBytes);
    }
    /** Constructs an instance of varbinary column. */
    public static LocalVarLenWriter<ByteArray> getInstanceVarbin(VirtualFile rawFile, int collectPerBytes) throws IOException {
        return new LocalVarLenWriter<ByteArray>(rawFile, new VarbinValueTraits(), collectPerBytes);
    }

    private final VarLenValueTraits<T> traits;

    private final int collectPerBytes;
    private int prevCollectPosition = -1; // to always collect at the first value
    private ArrayList<Integer> collectedTuples = new ArrayList<Integer>();
    private ArrayList<Integer> collectedPositions = new ArrayList<Integer>();
    public LocalVarLenWriter(VirtualFile file, VarLenValueTraits<T> traits) throws IOException {
        this (file, traits, 1 << 13);
    }
    public LocalVarLenWriter(VirtualFile file, VarLenValueTraits<T> traits, int collectPerBytes) throws IOException {
        super(file, traits, 1 << 18);
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
        if (prevCollectPosition < 0 || getRawCurPosition() - prevCollectPosition >= collectPerBytes) {
            collectedTuples.add(curTuple);
            collectedPositions.add(getRawCurPosition());
            prevCollectPosition = getRawCurPosition();
            assert (collectedTuples.size() == collectedPositions.size());
        }
    }

    @Override
    public void writePositionFile (VirtualFile posFile) throws IOException {
        LocalPosFile.createPosFile(posFile, collectedTuples, collectedPositions, curTuple, getRawCurPosition());
    }
}
