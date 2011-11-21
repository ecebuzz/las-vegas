package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.local.AllValueTraits.IntegerValueTraits;

public class LocalFixLenReaderIntegerTest extends LocalFixLenReaderTestBase<Integer, int[]> {
    @Override
    protected Integer generateValue(int index) { return (294493 * index) % (1 << 18);  }
    @Override
    protected FixLenValueTraits<Integer, int[]> createTraits() { return new IntegerValueTraits();}
    @Override
    protected int[] createArray (int size) { return new int[size];}
    @Override
    protected void setToArray(int[] array, int index, Integer value){ array[index] = value; }
    @Override
    protected Integer getFromArray(int[] array, int index) { return array[index]; }
}
