package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.traits.FixLenValueTraits;
import edu.brown.lasvegas.traits.SmallintValueTraits;

public class LocalBlockCompressionFixLenSmallintTest extends LocalBlockCompressionFixLenTestBase<Short, short[]> {
    @Override
    protected Short generateValue(int index) { return (short) ((409 * index) % (1L << 16));  }
    @Override
    protected FixLenValueTraits<Short, short[]> createTraits() { return new SmallintValueTraits();}
    @Override
    protected short[] createArray (int size) { return new short[size];}
    @Override
    protected void setToArray(short[] array, int index, Short value){ array[index] = value; }
    @Override
    protected Short getFromArray(short[] array, int index) { return array[index]; }
}
