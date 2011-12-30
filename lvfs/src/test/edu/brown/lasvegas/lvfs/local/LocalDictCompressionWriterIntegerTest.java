package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.IntegerValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictCompressionWriterIntegerTest extends LocalDictCompressionWriterTestBase4<Integer, int[]> {
    @Override
    protected Integer getOrderedValue(int order) {
        return (int) order;
    }
    @Override
    protected ValueTraits<Integer, int[]> getTraits() {
        return new IntegerValueTraits();
    }
}
