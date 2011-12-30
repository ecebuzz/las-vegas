package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueTraits;

public class LocalDictCompressionWriterFloatTest extends LocalDictCompressionWriterTestBase4<Float, float[]> {
    @Override
    protected Float getOrderedValue(int order) {
        return (float) order;
    }
    @Override
    protected ValueTraits<Float, float[]> getTraits() {
        return new AllValueTraits.FloatValueTraits();
    }
}
