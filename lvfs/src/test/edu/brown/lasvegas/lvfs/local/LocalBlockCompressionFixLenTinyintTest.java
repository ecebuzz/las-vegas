package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;

public class LocalBlockCompressionFixLenTinyintTest extends LocalBlockCompressionFixLenTestBase<Byte, byte[]> {
    @Override
    protected Byte generateValue(int index) { return (byte) (index - 50);  }
    @Override
    protected FixLenValueTraits<Byte, byte[]> createTraits() { return new AllValueTraits.TinyintValueTraits();}
    @Override
    protected byte[] createArray (int size) { return new byte[size];}
    @Override
    protected void setToArray(byte[] array, int index, Byte value){ array[index] = value; }
    @Override
    protected Byte getFromArray(byte[] array, int index) { return array[index]; }
}
