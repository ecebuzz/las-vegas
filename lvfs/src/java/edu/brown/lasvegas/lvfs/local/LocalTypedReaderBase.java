package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.PositionIndex;
import edu.brown.lasvegas.lvfs.RawValueReader;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;

/**
 * Base implementation of TypedReader. Doesn't do much. 
 */
public abstract class LocalTypedReaderBase<T extends Comparable<T>, AT> implements TypedReader<T, AT> {
    private final LocalRawFileReader rawReader;
    private final ValueTraits<T, AT> traits;
    protected LocalTypedReaderBase (VirtualFile file, ValueTraits<T, AT> traits, int streamBufferSize) throws IOException {
        this.rawReader = new LocalRawFileReader(file, streamBufferSize);
        this.traits = traits;
    }
    @Override
    public ValueTraits<T, AT> getValueTraits() {
        return traits;
    }

    public final LocalRawFileReader getRawReader () {
        return rawReader;
    }
    public final RawValueReader getRawValueReader () {
        return rawReader.getRawValueReader();
    }
    public final long getRawCurPosition() {
        return rawReader.getCurPosition();
    }

    /** Override this to add close-hook. */
    protected void beforeClose() throws IOException {}

    @Override
    public final void close() throws IOException {
        beforeClose();
        rawReader.close();
    }
    
    /** loads the specified position file to speed up seek. */
    public final void loadPositionFile (VirtualFile posFile) throws IOException {
        posIndex = new LocalPosFile(posFile);
    }
    @Override
    public void loadPositionIndex (PositionIndex posIndex) throws IOException {
        this.posIndex = posIndex;
    }
    
    protected PositionIndex posIndex;
}
