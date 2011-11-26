package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;

public class LocalRLEFloatTest extends LocalRLETestBase<Float, float[]> {
    @Override
    protected Float generateValue(int index) { return (float) ((923.8745 * (index / 10)) - (Math.pow(index, 1.47d)));  }
    @Override
    protected FixLenValueTraits<Float, float[]> createTraits() { return new AllValueTraits.FloatValueTraits();}
    @Override
    protected float[] createArray (int size) { return new float[size];}
    @Override
    protected void setToArray(float[] array, int index, Float value){ array[index] = value; }
    @Override
    protected Float getFromArray(float[] array, int index) { return array[index]; }
}
