package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.BigintValueTraits;
import edu.brown.lasvegas.traits.FixLenValueTraits;

public class LocalRLEBigintTest extends LocalRLETestBase<Long, long[]> {
    @Override
    protected Long generateValue(int index) { return (0xF32948D569843L * (index / 7)) % (1L << 40);  }
    @Override
    protected FixLenValueTraits<Long, long[]> createTraits() { return new BigintValueTraits();}
    @Override
    protected long[] createArray (int size) { return new long[size];}
    @Override
    protected void setToArray(long[] array, int index, Long value) { array[index] = value; }
    @Override
    protected Long getFromArray(long[] array, int index) { return array[index]; }
}
