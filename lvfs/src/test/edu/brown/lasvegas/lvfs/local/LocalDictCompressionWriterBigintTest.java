package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueTraits;

public class LocalDictCompressionWriterBigintTest extends LocalDictCompressionWriterTestBase4<Long, long[]> {
    @Override
    protected Long getOrderedValue(int order) {
        return (long) order;
    }
    @Override
    protected ValueTraits<Long, long[]> getTraits() {
        return new AllValueTraits.BigintValueTraits();
    }
}
