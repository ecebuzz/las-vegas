package edu.brown.lasvegas.lvfs.local;

public class LocalFixLenReaderTinyintTest extends LocalFixLenReaderTestBase<Byte, byte[]> {
    @Override
    protected Byte generateValue(int index) { return (byte) (index - 50);  }
    @Override
    protected FixLenValueTraits<Byte, byte[]> createTraits() { return new AllValueTraits.TinyintValueTraits();}
    @Override
    protected byte[] createArray (int size) { return new byte[size];}
    @Override
    protected Byte getFromArray(byte[] array, int index) { return array[index]; }
}
