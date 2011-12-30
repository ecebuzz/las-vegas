package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.VarLenValueTraits;
import edu.brown.lasvegas.traits.VarbinValueTraits;
import edu.brown.lasvegas.util.ByteArray;

public class LocalRLEVarbinTest extends LocalRLETestBase<ByteArray, ByteArray[]> {
    // only varbin is [] by itself. to avoid reference comparison, this overrides assertEqualsT
    @Override
    protected void assertEqualsT (ByteArray a, ByteArray b) {
        assertEquals (a, b);
    }
    @Override
    protected ByteArray generateValue(int index) { return new ByteArray(("str" + (index / 8) + "sdfdf").getBytes());  }
    @Override
    protected VarLenValueTraits<ByteArray> createTraits() { return new VarbinValueTraits();}
    @Override
    protected ByteArray[] createArray (int size) { return new ByteArray[size];}
    @Override
    protected void setToArray(ByteArray[] array, int index, ByteArray value){ array[index] = value; }
    @Override
    protected ByteArray getFromArray(ByteArray[] array, int index) { return array[index]; }

    @SuppressWarnings("unused")
    @Test
    public void testRunCount() throws IOException {
        assertEquals(VALUE_COUNT / 8 + (VALUE_COUNT % 8 == 0 ? 0 : 1), super.runCount);
    }
}
