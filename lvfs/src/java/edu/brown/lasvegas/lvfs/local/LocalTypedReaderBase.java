package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;

import edu.brown.lasvegas.lvfs.RawValueReader;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.ValueTraits;

/**
 * Base implementation of TypedReader. Doesn't do much. 
 */
public abstract class LocalTypedReaderBase<T, AT> implements TypedReader<T, AT> {
    private final LocalRawFileReader rawReader;
    //private final ValueTraits<T, AT> traits;
    protected LocalTypedReaderBase (File file, ValueTraits<T, AT> traits, int streamBufferSize) throws IOException {
        this.rawReader = new LocalRawFileReader(file, streamBufferSize);
        //this.traits = traits;
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
}
