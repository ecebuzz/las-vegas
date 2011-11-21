package edu.brown.lasvegas.lvfs.local;

public class LocalFixLenReaderFloatTest extends LocalFixLenReaderTestBase<Float, float[]> {
    @Override
    protected Float generateValue(int index) { return (float) ((923.8745 * index) - (Math.pow(index, 1.47d)));  }
    @Override
    protected FixLenValueTraits<Float, float[]> createTraits() { return new AllValueTraits.FloatValueTraits();}
    @Override
    protected float[] createArray (int size) { return new float[size];}
    @Override
    protected Float getFromArray(float[] array, int index) { return array[index]; }
}
