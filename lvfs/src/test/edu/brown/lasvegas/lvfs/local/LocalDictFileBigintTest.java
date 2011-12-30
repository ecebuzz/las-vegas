package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.BigintValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictFileBigintTest extends LocalDictFileTestBase4<Long, long[]> {
    @Override
    protected Long generateValue(int index, int dv) {
        return (long) (randoms[index] % dv);
    }
    @Override
    protected TypedReader<Long, long[]> getReader(VirtualFile file) throws IOException {
        return LocalFixLenReader.getInstanceBigint(file);
    }
    @Override
    protected TypedWriter<Long, long[]> getWriter(VirtualFile file, int collectPerLongs) throws IOException {
        return LocalFixLenWriter.getInstanceBigint(file);
    }
    @Override
    protected ValueTraits<Long, long[]> getTraits() {
        return new BigintValueTraits();
    }
}
