package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.FloatValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictFileFloatTest extends LocalDictFileTestBase4<Float, float[]> {
    @Override
    protected Float generateValue(int index, int dv) {
        return Float.intBitsToFloat(randoms[index] % dv);
    }
    @Override
    protected TypedReader<Float, float[]> getReader(VirtualFile file) throws IOException {
        return LocalFixLenReader.getInstanceFloat(file);
    }
    @Override
    protected TypedWriter<Float, float[]> getWriter(VirtualFile file, int collectPerFloats) throws IOException {
        return LocalFixLenWriter.getInstanceFloat(file);
    }
    @Override
    protected ValueTraits<Float, float[]> getTraits() {
        return new FloatValueTraits();
    }
}
