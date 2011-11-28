package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.VarLenValueTraits;

public class LocalRLEVarbinTest extends LocalRLETestBase<byte[], byte[][]> {
    // only varbin is [] by itself. to avoid reference comparison, this overrides assertEqualsT
    @Override
    protected void assertEqualsT (byte[] a, byte[] b) {
        assertArrayEquals (a, b);
    }
    @Override
    protected byte[] generateValue(int index) { return ("str" + (index / 8) + "sdfdf").getBytes();  }
    @Override
    protected VarLenValueTraits<byte[]> createTraits() { return new AllValueTraits.VarbinValueTraits();}
    @Override
    protected byte[][] createArray (int size) { return new byte[size][];}
    @Override
    protected void setToArray(byte[][] array, int index, byte[] value){ array[index] = value; }
    @Override
    protected byte[] getFromArray(byte[][] array, int index) { return array[index]; }

    @SuppressWarnings("unused")
    @Test
    public void testRunCount() throws IOException {
        assertEquals(VALUE_COUNT / 8 + (VALUE_COUNT % 8 == 0 ? 0 : 1), super.runCount);
    }
}
