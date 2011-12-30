package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

public class LocalDictFileSmallintTest extends LocalDictFileTestBase2<Short, short[]> {
    @Override
    protected Short generateValue(int index, int dv) {
        return (short) (randoms[index] % dv);
    }
    @Override
    protected TypedReader<Short, short[]> getReader(VirtualFile file) throws IOException {
        return LocalFixLenReader.getInstanceSmallint(file);
    }
    @Override
    protected TypedWriter<Short, short[]> getWriter(VirtualFile file, int collectPerShorts) throws IOException {
        return LocalFixLenWriter.getInstanceSmallint(file);
    }
    @Override
    protected ValueTraits<Short, short[]> getTraits() {
        return new AllValueTraits.SmallintValueTraits();
    }
}
