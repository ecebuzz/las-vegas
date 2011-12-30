package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueTraits;

public class LocalDictCompressionWriterTinyintTest extends LocalDictCompressionWriterTestBase1<Byte, byte[]> {
    @Override
    protected Byte getOrderedValue(int order) {
        return (byte) order;
    }
    @Override
    protected ValueTraits<Byte, byte[]> getTraits() {
        return new AllValueTraits.TinyintValueTraits();
    }
}
