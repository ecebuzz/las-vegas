package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

public class LocalDictFileDoubleTest extends LocalDictFileTestBase4<Double, double[]> {
    @Override
    protected Double generateValue(int index, int dv) {
        return Double.longBitsToDouble(randoms[index] % dv);
    }
    @Override
    protected TypedReader<Double, double[]> getReader(VirtualFile file) throws IOException {
        return LocalFixLenReader.getInstanceDouble(file);
    }
    @Override
    protected TypedWriter<Double, double[]> getWriter(VirtualFile file, int collectPerDoubles) throws IOException {
        return LocalFixLenWriter.getInstanceDouble(file);
    }
    @Override
    protected ValueTraits<Double, double[]> getTraits() {
        return new AllValueTraits.DoubleValueTraits();
    }
}