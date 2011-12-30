package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueTraits;

public class LocalDictCompressionWriterVarcharTest extends LocalDictCompressionWriterTestBase4<String, String[]> {
    @Override
    protected String getOrderedValue(int order) {
        return "key-" + String.format("%06d", order);
    }
    @Override
    protected ValueTraits<String, String[]> getTraits() {
        return new AllValueTraits.VarcharValueTraits();
    }
}
