package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueTraits;

public class LocalDictCompressionWriterSmallintTest extends LocalDictCompressionWriterTestBase2<Short, short[]> {
    @Override
    protected Short getOrderedValue(int order) {
        return (short) order;
    }
    @Override
    protected ValueTraits<Short, short[]> getTraits() {
        return new AllValueTraits.SmallintValueTraits();
    }
}
