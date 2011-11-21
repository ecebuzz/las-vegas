package edu.brown.lasvegas.lvfs.local;

public class LocalFixLenReaderSmallintTest extends LocalFixLenReaderTestBase<Short, short[]> {
    @Override
    protected Short generateValue(int index) { return (short) ((409 * index) % (1L << 16));  }
    @Override
    protected FixLenValueTraits<Short, short[]> createTraits() { return new AllValueTraits.SmallintValueTraits();}
    @Override
    protected short[] createArray (int size) { return new short[size];}
    @Override
    protected Short getFromArray(short[] array, int index) { return array[index]; }
}
