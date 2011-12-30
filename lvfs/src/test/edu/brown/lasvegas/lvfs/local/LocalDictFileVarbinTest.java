package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.VarbinValueTraits;
import edu.brown.lasvegas.util.ByteArray;

public class LocalDictFileVarbinTest extends LocalDictFileTestBase4<ByteArray, ByteArray[]> {
    @Override
    protected ByteArray generateValue(int index, int dv) {
        // to keep the <, > relationship, pad with zeros
        int rand = randoms[index] % dv;
        return new ByteArray(new byte[] {(byte) (rand >> 24), (byte) ((rand >> 16) & 0xFF), (byte) ((rand >> 8) & 0xFF), (byte) (rand & 0xFF)});
    }
    @Override
    protected TypedReader<ByteArray, ByteArray[]> getReader(VirtualFile file) throws IOException {
        return LocalVarLenReader.getInstanceVarbin(file);
    }
    @Override
    protected TypedWriter<ByteArray, ByteArray[]> getWriter(VirtualFile file, int collectPerBytes) throws IOException {
        return LocalVarLenWriter.getInstanceVarbin(file, collectPerBytes);
    }
    @Override
    protected ValueTraits<ByteArray, ByteArray[]> getTraits() {
        return new VarbinValueTraits();
    }
}
