package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.VarcharValueTraits;

public class LocalDictFileVarcharTest extends LocalDictFileTestBase4<String, String[]> {
    @Override
    protected String generateValue(int index, int dv) {
        // to keep the <, > relationship, pad with zeros
        String paddedRand = String.format("%08d", randoms[index] % dv);
        assert (paddedRand.indexOf('-') < 0);
        return ("str" + paddedRand + "ad");
    }
    @Override
    protected TypedReader<String, String[]> getReader(VirtualFile file) throws IOException {
        return LocalVarLenReader.getInstanceVarchar(file);
    }
    @Override
    protected TypedWriter<String, String[]> getWriter(VirtualFile file, int collectPerBytes) throws IOException {
        return LocalVarLenWriter.getInstanceVarchar(file, collectPerBytes);
    }
    @Override
    protected ValueTraits<String, String[]> getTraits() {
        return new VarcharValueTraits();
    }
}
