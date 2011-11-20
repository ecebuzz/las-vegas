package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

/**
 * LocalRawFileReader for variable-length entries.
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
public class LocalVarLenRawFileReader<T> extends LocalRawFileReader implements TypedReader<T, T[]> {
    private final VarLenValueTraits<T> traits;

    /** Constructs an instance of varchar column. */
    public static LocalVarLenRawFileReader<String> getInstanceVarchar(File rawFile) throws IOException {
        return new LocalVarLenRawFileReader<String>(rawFile, new AllValueTraits.VarcharValueTraits());
    }
    /** Constructs an instance of varbinary column. */
    public static LocalVarLenRawFileReader<byte[]> getInstanceVarbin(File rawFile) throws IOException {
        return new LocalVarLenRawFileReader<byte[]>(rawFile, new AllValueTraits.VarbinValueTraits());
    }

    private LocalVarLenRawFileReader(File rawFile, VarLenValueTraits<T> traits) throws IOException {
        super (rawFile);
        this.traits = traits;
    }

    @Override
    public T readValue () throws IOException {
        int length = readLengthHeader();
        return traits.readValue(this, length);
    }

    @Override
    public void skipValue () throws IOException {
        int length = readLengthHeader();
        super.seekToByteRelative(length);
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
