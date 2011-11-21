package edu.brown.lasvegas.lvfs.local;

public class LocalFixLenReaderBigintTest extends LocalFixLenReaderTestBase<Long, long[]> {
    @Override
    protected Long generateValue(int index) { return (0xF32948D569843L * index) % (1L << 40);  }
    @Override
    protected FixLenValueTraits<Long, long[]> createTraits() { return new AllValueTraits.BigintValueTraits();}
    @Override
    protected long[] createArray (int size) { return new long[size];}
    @Override
    protected Long getFromArray(long[] array, int index) { return array[index]; }
}
