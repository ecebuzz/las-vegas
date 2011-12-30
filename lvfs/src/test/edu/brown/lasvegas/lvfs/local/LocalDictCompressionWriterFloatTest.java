package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.FloatValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictCompressionWriterFloatTest extends LocalDictCompressionWriterTestBase4<Float, float[]> {
    @Override
    protected Float getOrderedValue(int order) {
        return (float) order;
    }
    @Override
    protected ValueTraits<Float, float[]> getTraits() {
        return new FloatValueTraits();
    }
}
