package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.DoubleValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictCompressionWriterDoubleTest extends LocalDictCompressionWriterTestBase4<Double, double[]> {
    @Override
    protected Double getOrderedValue(int order) {
        return (double) order;
    }
    @Override
    protected ValueTraits<Double, double[]> getTraits() {
        return new DoubleValueTraits();
    }
}
