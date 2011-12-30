package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueTraits;

public class LocalDictCompressionWriterDoubleTest extends LocalDictCompressionWriterTestBase4<Double, double[]> {
    @Override
    protected Double getOrderedValue(int order) {
        return (double) order;
    }
    @Override
    protected ValueTraits<Double, double[]> getTraits() {
        return new AllValueTraits.DoubleValueTraits();
    }
}
