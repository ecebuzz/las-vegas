package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedRLEWriter;
import edu.brown.lasvegas.lvfs.ValueRun;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.util.ByteArray;

/**
 * File writer for RLE compressed column.
 * <p>The file format is a series of pairs, run-length and value.</p>
 * <p>Run-length is always a 4 byte integer while value might be fixed length
 * or variable length. The difference is implemented in traits object.</p>
 * <p>In either case, it is not trivial to seek or tell the total tuple count
 * by this file itself. So, just like LocalVarLenWriter, RLE file also outputs a position file.</p>
 */
public final class LocalRLEWriter<T extends Comparable<T>, AT> extends LocalTypedWriterBase<T, AT> implements TypedRLEWriter<T, AT> {
    /** Constructs an instance for 1-byte fixed length integer values. */
    public static LocalRLEWriter<Byte, byte[]> getInstanceTinyint(VirtualFile rawFile) throws IOException {
        return new LocalRLEWriter<Byte, byte[]>(rawFile, new AllValueTraits.TinyintValueTraits());
    }
    /** Constructs an instance for 2-byte fixed length integer values. */
    public static LocalRLEWriter<Short, short[]> getInstanceSmallint(VirtualFile rawFile) throws IOException {
        return new LocalRLEWriter<Short, short[]>(rawFile, new AllValueTraits.SmallintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length integer values. */
    public static LocalRLEWriter<Integer, int[]> getInstanceInteger(VirtualFile rawFile) throws IOException {
        return new LocalRLEWriter<Integer, int[]>(rawFile, new AllValueTraits.IntegerValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length integer values. */
    public static LocalRLEWriter<Long, long[]> getInstanceBigint(VirtualFile rawFile) throws IOException {
        return new LocalRLEWriter<Long, long[]>(rawFile, new AllValueTraits.BigintValueTraits());
    }
    /** Constructs an instance for 4-byte fixed length float values. */
    public static LocalRLEWriter<Float, float[]> getInstanceFloat(VirtualFile rawFile) throws IOException {
        return new LocalRLEWriter<Float, float[]>(rawFile, new AllValueTraits.FloatValueTraits());
    }
    /** Constructs an instance for 8-byte fixed length float values. */
    public static LocalRLEWriter<Double, double[]> getInstanceDouble(VirtualFile rawFile) throws IOException {
        return new LocalRLEWriter<Double, double[]>(rawFile, new AllValueTraits.DoubleValueTraits());
    }

    /** Constructs an instance of varchar column. */
    public static LocalRLEWriter<String, String[]> getInstanceVarchar(VirtualFile dataFile) throws IOException {
        return new LocalRLEWriter<String, String[]>(dataFile, new AllValueTraits.VarcharValueTraits());
    }
    /** Constructs an instance of varbinary column. */
    public static LocalRLEWriter<ByteArray, ByteArray[]> getInstanceVarbin(VirtualFile dataFile) throws IOException {
        return new LocalRLEWriter<ByteArray, ByteArray[]>(dataFile, new AllValueTraits.VarbinValueTraits());
    }

    
    private static Logger LOG = Logger.getLogger(LocalRLEWriter.class);
    private final ValueTraits<T, AT> traits;

    private ValueRun<T> curRun = new ValueRun<T>();

    private final int collectPerBytes;
    private int prevCollectPosition = -1; // to always collect at the first value
    private int curTuple = 0;
    private ArrayList<Integer> collectedTuples = new ArrayList<Integer>();
    private ArrayList<Integer> collectedPositions = new ArrayList<Integer>();
    /** just a statistics. */
    private int runCount = 0;

    public LocalRLEWriter(VirtualFile file, ValueTraits<T, AT> traits) throws IOException {
        this (file, traits, 1 << 10);
    }
    public LocalRLEWriter(VirtualFile file, ValueTraits<T, AT> traits, int collectPerBytes) throws IOException {
        this (file, traits, collectPerBytes, 1 << 16);
    }
    public LocalRLEWriter(VirtualFile file, ValueTraits<T, AT> traits, int collectPerBytes, int streamBufferSize) throws IOException {
        super (file, traits, streamBufferSize);
        this.traits = traits;
        this.collectPerBytes = collectPerBytes;
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
    public ValueRun<T> startNewRun (T value, int newRunLength) throws IOException {
        // write out current run
        collectTuplePosition(); // might collect
        if (curRun.runLength > 0) {
            getRawValueWriter().writeInt(curRun.runLength);
            traits.writeValue(getRawValueWriter(), curRun.value);
            ++runCount;
        }

        // start a new run
        int oldRunLength = curRun.runLength;
        curRun.value = value;
        curRun.runLength = newRunLength;
        
        // then increment curTuple (_after_ collectTuplePosition(). notice we don't do this in writeValue())
        curTuple += oldRunLength;
        return curRun;
    }
    @Override
    public ValueRun<T> getCurrentRun() {
        return curRun;
    }
    
    @Override
    public final void writeValue(T value) throws IOException {
        if (curRun.value == null || !curRun.value.equals(value)) {
            startNewRun(value, 1);
        } else {
            ++curRun.runLength;
        }
    }
    
    @Override
    public final void writeValues(AT values, int off, int len) throws IOException {
        if (len == 0) return;
        if (curRun.runLength == 0) {
            // make sure there is a current run
            startNewRun(traits.get(values, off), 1);
            ++off;
            --len;
        }
        if (len == 0) return;
        traits.writeRunLengthes(this, values, off, len);
    }

    @Override
    public final long writeFileFooter() throws IOException {
        // this is not quite a file footer, but we need to flush the last run here.
        startNewRun(null, 0); // flush the current run
        return getRawValueWriter().getCRC32Value();
    }
    
    /**
     * Returns the number of runs we have so far output.
     * After writeFileFooter(), this gives the exact number of runs in this file.
     */
    public final int getRunCount () {
        return runCount;
    }
    
    @Override
    protected final void beforeClose() throws IOException {
        // just a debug out
        if (curRun.runLength > 0) {
            // just warn. the user might have simply canceled writing this file 
            LOG.warn("the last run (" + curRun + ") has not been flushed before close(). have you called writeFileFooter()? : " + this);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug ("Applied RLE. " + curTuple + " tuples to " + runCount + " runs. " + getRawCurPosition() + " bytes");
        }
    }

    /**
     * Writes out the collected positions to a position file.
     */
    public void writePositionFile (VirtualFile posFile) throws IOException {
        LocalPosFile.createPosFile(posFile, collectedTuples, collectedPositions, curTuple, getRawCurPosition());
    }
}
