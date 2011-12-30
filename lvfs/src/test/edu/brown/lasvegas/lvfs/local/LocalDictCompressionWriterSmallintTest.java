package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.SmallintValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictCompressionWriterSmallintTest extends LocalDictCompressionWriterTestBase2<Short, short[]> {
    @Override
    protected Short getOrderedValue(int order) {
        return (short) order;
    }
    @Override
    protected ValueTraits<Short, short[]> getTraits() {
        return new SmallintValueTraits();
    }
}
