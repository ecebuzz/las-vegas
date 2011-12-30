package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.VarcharValueTraits;

public class LocalDictCompressionWriterVarcharTest extends LocalDictCompressionWriterTestBase4<String, String[]> {
    @Override
    protected String getOrderedValue(int order) {
        return "key-" + String.format("%06d", order);
    }
    @Override
    protected ValueTraits<String, String[]> getTraits() {
        return new VarcharValueTraits();
    }
}
