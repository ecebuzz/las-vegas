package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VarLenValueTraits;
import edu.brown.lasvegas.lvfs.local.LocalPosFile.Pos;

/**
 * File reader for variable-length entries.
 * The file format is a series of:
 * <table>
 * <tr><td>size of length</td><td>1 byte. 1/2/4/8.</td></tr>
 * <tr><td>length</td><td>1/2/4/8 bytes. (1)</td></tr>
 * <tr><td>data</td><td><i>length</i> bytes.</td></tr>
 * </table>
 * 
 * <p>(1) 8 bytes entry is not allowed so far. Needs to extend the whole API
 * because we cannot return such a large value as a byte array.</p>
 * 
 * <p>This file reader cannot jump to arbitrary tuple position by itself.
 * Give a position file ({@link LocalPosFileReader}) to the constructor to speed it up.</p>
 */
public final class LocalVarLenReader<T> extends LocalRawFileReader implements TypedReader<T, T[]> {
    private static Logger LOG = Logger.getLogger(LocalVarLenReader.class);
    private final VarLenValueTraits<T> traits;

    /** Constructs an instance of varchar column. */
    public static LocalVarLenReader<String> getInstanceVarchar(File dataFile) throws IOException {
        return new LocalVarLenReader<String>(dataFile, new AllValueTraits.VarcharValueTraits());
    }
    /** Constructs an instance of varchar column with position file. */
    public static LocalVarLenReader<String> getInstanceVarchar(File dataFile, File posFile) throws IOException {
        return new LocalVarLenReader<String>(dataFile, posFile, new AllValueTraits.VarcharValueTraits());
    }
    /** Constructs an instance of varbinary column. */
    public static LocalVarLenReader<byte[]> getInstanceVarbin(File dataFile) throws IOException {
        return new LocalVarLenReader<byte[]>(dataFile, new AllValueTraits.VarbinValueTraits());
    }
    /** Constructs an instance of varbinary column with position file. */
    public static LocalVarLenReader<byte[]> getInstanceVarbin(File dataFile, File posFile) throws IOException {
        return new LocalVarLenReader<byte[]>(dataFile, posFile, new AllValueTraits.VarbinValueTraits());
    }

    public LocalVarLenReader(File dataFile, VarLenValueTraits<T> traits) throws IOException {
        this (dataFile, null, traits);
    }
    public LocalVarLenReader(File dataFile, File posFile, VarLenValueTraits<T> traits) throws IOException {
        this (dataFile, posFile, traits, 1 << 16);
    }
    /**
     * Constructs a variable-length file reader. The optional position file speeds up seeking
     * @param dataFile required. the main data file
     * @param posFile optional. position file to speed up locating tuple. (without it, seeking will be terribly slow)
     */
    public LocalVarLenReader(File dataFile, File posFile, VarLenValueTraits<T> traits, int streamBufferSize) throws IOException {
        super (dataFile, streamBufferSize);
        assert (dataFile != null);
        this.traits = traits;
        if (posFile != null) {
            posIndex = new LocalPosFile(posFile);
        } else {
            posIndex = null;
        }
    }
    private final LocalPosFile posIndex;
    private int curTuple = 0;

    @Override
    public T readValue () throws IOException {
        T value = traits.readValue(getRawValueReader());
        ++curTuple;
        return value;
    }
    @Override
    public int readValues(T[] buffer, int off, int len) throws IOException {
        // unlike fixed-len reader. there is no faster way to do this.
        // so, just call readValue for each value...
        int count = 0;
        for (; count < len && getCurPosition() < rawFileSize; ++count) {
            buffer[off + count] = readValue();
        }
        return count;
    }

    @Override
    public void skipValue () throws IOException {
        int length = getRawValueReader(). readLengthHeader();
        getRawValueReader().skipBytes(length);
        ++curTuple;
    }
    @Override
    public void skipValues(int skip) throws IOException {
        seekToTupleAbsolute (curTuple + skip);
    }

    @Override
    public void seekToTupleAbsolute (int tuple) throws IOException {
        if (posIndex == null) {
            LOG.warn("seeking to " + tuple + "th tuple without position file. this will be terribly slow!");
            if (tuple < curTuple) {
                // we have to first seek to the beginning
                seekToByteAbsolute(0);
                curTuple = 0;
            }
            while (tuple > curTuple) {
                skipValue();
            }
        } else {
            // seek using the position file
            Pos pos = posIndex.searchPosition(tuple);
            seekToByteAbsolute(pos.bytePosition);
            curTuple = (int) pos.tuple;
            // remaining is sequential search
            while (tuple > curTuple) {
                skipValue();
            }
        }
    }

    @Override
    public int getTotalTuples () {
        // we can answer the number of tuples only when position file is given
        if (posIndex == null) {
            throw new NotImplementedException();
        } else {
            return posIndex.getTotalTuples();
        }
    }
}
