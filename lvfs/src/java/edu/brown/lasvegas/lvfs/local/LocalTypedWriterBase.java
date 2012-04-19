package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.RawValueWriter;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;

/**
 * Base implementation of TypedWriter. Doesn't do much. 
 */
public abstract class LocalTypedWriterBase<T extends Comparable<T>, AT> implements TypedWriter<T, AT> {
    private final LocalRawFileWriter rawWriter;
    private final ValueTraits<T, AT> traits;
    protected int curTuple;
    protected LocalTypedWriterBase (VirtualFile file, ValueTraits<T, AT> traits, int streamBufferSize) throws IOException {
        this.rawWriter = new LocalRawFileWriter(file, streamBufferSize);
        this.traits = traits;
        this.curTuple = 0;
    }
    
    @Override
    public ValueTraits<T, AT> getValueTraits() {
        return traits;
    }

    @Override
    public void setCRC32Enabled(boolean enabled) {
        rawWriter.getRawValueWriter().setCRC32Enabled(enabled);
    }
    @Override
    public long writeFileFooter() throws IOException {
        return rawWriter.getRawValueWriter().getCRC32Value();
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
    @Override
    public final int getTupleCount() {
    	return curTuple;
    }
}
