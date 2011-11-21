package edu.brown.lasvegas.lvfs.local;

public class LocalFixLenReaderDoubleTest extends LocalFixLenReaderTestBase<Double, double[]> {
    @Override
    protected Double generateValue(int index) { return (923482.87453457 * index) - (Math.pow(index, 1.47d));  }
    @Override
    protected FixLenValueTraits<Double, double[]> createTraits() { return new AllValueTraits.DoubleValueTraits();}
    @Override
    protected double[] createArray (int size) { return new double[size];}
    @Override
    protected void setToArray(double[] array, int index, Double value) { array[index] = value; }
    @Override
    protected Double getFromArray(double[] array, int index) { return array[index]; }
}
