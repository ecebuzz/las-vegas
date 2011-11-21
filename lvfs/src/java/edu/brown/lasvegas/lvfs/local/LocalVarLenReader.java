package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

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
 * Use {@link LocalPosFileReader} and {@link #seekToByteAbsolute(long)}
 * to do so.</p>
 */
public class LocalVarLenReader<T> extends LocalTypedReader<T, T[]> {
    private final VarLenValueTraits<T> traits;

    /** Constructs an instance of varchar column. */
    public static LocalVarLenReader<String> getInstanceVarchar(File rawFile) throws IOException {
        return new LocalVarLenReader<String>(rawFile, new AllValueTraits.VarcharValueTraits());
    }
    /** Constructs an instance of varbinary column. */
    public static LocalVarLenReader<byte[]> getInstanceVarbin(File rawFile) throws IOException {
        return new LocalVarLenReader<byte[]>(rawFile, new AllValueTraits.VarbinValueTraits());
    }

    public LocalVarLenReader(File rawFile, VarLenValueTraits<T> traits) throws IOException {
        super (rawFile);
        this.traits = traits;
    }

    @Override
    public T readValue () throws IOException {
        int length = readLengthHeader();
        return traits.readValue(this, length);
    }
    @Override
    public int readValues(T[] buffer, int off, int len) throws IOException {
        // unlike fixed-len reader. there is no faster way to do this.
        // so, just call readValue for each value...
        int count = 0;
        for (; count < len; ++count) {
            buffer[off + count] = readValue();
        }
        return count;
    }

    @Override
    public void skipValue () throws IOException {
        int length = readLengthHeader();
        super.seekToByteRelative(length);
    }
    @Override
    public void skipValues(int skip) throws IOException {
        for (int i = 0; i < skip; ++i) {
            skipValue();
        }
    }
    private int readLengthHeader () throws IOException {
        byte lengthSize = super.readByte();
        int length;
        switch (lengthSize) {
        case 1: length = super.readByte(); break;
        case 2: length = super.readShort(); break;
        case 4: length = super.readInt(); break;
        default:
            throw new IOException ("unexpected length size=" + lengthSize + ". corrupted file?" + this);
        }
        if (length < 0) {
            throw new IOException ("negative value length=" + length + ". corrupted file?" + this);
        }
        return length;
    }
}
