package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

public class LocalDictFileTinyintTest extends LocalDictFileTestBase1<Byte, byte[]> {
    @Override
    protected Byte generateValue(int index, int dv) {
        return (byte) (randoms[index] % dv);
    }
    @Override
    protected TypedReader<Byte, byte[]> getReader(VirtualFile file) throws IOException {
        return LocalFixLenReader.getInstanceTinyint(file);
    }
    @Override
    protected TypedWriter<Byte, byte[]> getWriter(VirtualFile file, int collectPerBytes) throws IOException {
        return LocalFixLenWriter.getInstanceTinyint(file);
    }
    @Override
    protected ValueTraits<Byte, byte[]> getTraits() {
        return new AllValueTraits.TinyintValueTraits();
    }
}
