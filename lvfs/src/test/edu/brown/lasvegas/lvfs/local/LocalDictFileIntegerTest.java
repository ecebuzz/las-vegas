package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.IntegerValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictFileIntegerTest extends LocalDictFileTestBase4<Integer, int[]> {
    @Override
    protected Integer generateValue(int index, int dv) {
        return (randoms[index] % dv);
    }
    @Override
    protected TypedReader<Integer, int[]> getReader(VirtualFile file) throws IOException {
        return LocalFixLenReader.getInstanceInteger(file);
    }
    @Override
    protected TypedWriter<Integer, int[]> getWriter(VirtualFile file, int collectPerIntegers) throws IOException {
        return LocalFixLenWriter.getInstanceInteger(file);
    }
    @Override
    protected ValueTraits<Integer, int[]> getTraits() {
        return new IntegerValueTraits();
    }
}
