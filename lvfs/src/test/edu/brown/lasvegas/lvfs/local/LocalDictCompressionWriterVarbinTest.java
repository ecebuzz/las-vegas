package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.VarbinValueTraits;
import edu.brown.lasvegas.util.ByteArray;

public class LocalDictCompressionWriterVarbinTest extends LocalDictCompressionWriterTestBase4<ByteArray, ByteArray[]> {
    @Override
    protected ByteArray getOrderedValue(int order) {
        return new ByteArray(new byte[] {(byte) (order >> 24), (byte) ((order >> 16) & 0xFF), (byte) ((order >> 8) & 0xFF), (byte) (order & 0xFF)});
    }
    @Override
    protected ValueTraits<ByteArray, ByteArray[]> getTraits() {
        return new VarbinValueTraits();
    }
}
