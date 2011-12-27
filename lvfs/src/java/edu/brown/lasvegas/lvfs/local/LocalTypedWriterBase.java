package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.RawValueWriter;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Base implementation of TypedWriter. Doesn't do much. 
 */
public abstract class LocalTypedWriterBase<T, AT> implements TypedWriter<T, AT> {
    private final LocalRawFileWriter rawWriter;
    //private final ValueTraits<T, AT> traits;
    protected LocalTypedWriterBase (VirtualFile file, ValueTraits<T, AT> traits, int streamBufferSize) throws IOException {
        this.rawWriter = new LocalRawFileWriter(file, streamBufferSize);
        //this.traits = traits;
    }

    public final RawValueWriter getRawValueWriter () {
        return rawWriter.getRawValueWriter();
    }
    public final int getRawCurPosition() {
        return rawWriter.getCurPosition();
    }

    /** Override this to add flush-hook. */
    protected void beforeFlush() throws IOException {}

    @Override
    public final void flush() throws IOException {
        beforeFlush();
        rawWriter.flush();
    }
    @Override
    public final void flush(boolean sync) throws IOException {
        beforeFlush();
        rawWriter.flush(sync);
    }

    /** Override this to add close-hook. */
    protected void beforeClose() throws IOException {}

    @Override
    public final void close() throws IOException {
        beforeClose();
        rawWriter.close();
    }

    /** Override this to specific file footer. */
    @Override
    public void writeFileFooter() throws IOException {}
}
