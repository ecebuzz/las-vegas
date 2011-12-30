package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.TinyintValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;

public class LocalDictCompressionWriterTinyintTest extends LocalDictCompressionWriterTestBase1<Byte, byte[]> {
    @Override
    protected Byte getOrderedValue(int order) {
        return (byte) order;
    }
    @Override
    protected ValueTraits<Byte, byte[]> getTraits() {
        return new TinyintValueTraits();
    }
}
