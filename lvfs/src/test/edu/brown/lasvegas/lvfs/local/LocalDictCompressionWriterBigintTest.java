package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.BigintValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictCompressionWriterBigintTest extends LocalDictCompressionWriterTestBase4<Long, long[]> {
    @Override
    protected Long getOrderedValue(int order) {
        return (long) order;
    }
    @Override
    protected ValueTraits<Long, long[]> getTraits() {
        return new BigintValueTraits();
    }
}
