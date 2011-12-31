package edu.brown.lasvegas.traits;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class DoubleValueTraitsTest {
    private DoubleValueTraits traits;
    @Before
    public void setUp () {
        traits = new DoubleValueTraits();
    }
    @Test
    public void testGetBitsPerValue() {
        assertEquals (64, traits.getBitsPerValue());
    }


    @Test
    public void testCreateArray() {
        assertEquals (20, traits.createArray(20).length);
    }

    @Test
    public void testLength() {
        assertEquals (20, traits.length(traits.createArray(20)));
    }

    @Test
    public void testToArray() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        List<Double> list = new ArrayList<Double>();
        for (double val : values) list.add(val);
        double[] array = traits.toArray(list);
        assertArrayEquals(values, array, 0.000001d);
    }

    @Test
    public void testBinarySearch() {
        double[] values = new double[123];
        for (int i = 0; i < values.length; ++i) {
            values[i] = i * 23;
        }
        assertEquals (20, traits.binarySearch(values, (double) (20 * 23)));
        assertEquals (- 21 - 1, traits.binarySearch(values, (double) (20 * 23) + 1));
        assertEquals (- 20 - 1, traits.binarySearch(values, (double) (20 * 23) - 1));
        assertEquals (0, traits.binarySearch(values, 0d));
        assertEquals (- 1 - 1, traits.binarySearch(values, 1d));
        assertEquals (-1, traits.binarySearch(values, -1d));
        assertEquals (values.length - 1, traits.binarySearch(values, (double) ((values.length - 1) * 23)));
        assertEquals (- values.length + 1 - 1, traits.binarySearch(values, (double) ((values.length - 1) * 23) - 1));
        assertEquals (- values.length - 1, traits.binarySearch(values, (double) ((values.length - 1) * 23) + 1));
    }

    @Test
    public void testSortDoubleArray() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        double[] answer = new double[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        traits.sort(values);
        assertArrayEquals(answer, values, 0.000001d);
    }

    @Test
    public void testSortDoubleArrayIntInt() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        double[] answer = new double[]{234,234,-234234234,4553,145742,245646,4547457,3434};
        traits.sort(values, 2, 7);
        assertArrayEquals(answer, values, 0.000001d);
    }

    @Test
    public void testSortKeyValueDoubleArrayIntArray() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        double[] answer = new double[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        traits.sortKeyValue(values, pos);
        assertArrayEquals(answer, values, 0.000001d);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testSortKeyValueDoubleArrayIntArrayIntInt() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        double[] answer = new double[]{234,234,-234234234,4553,145742,245646,4547457,3434};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{0,1,2,3,5,6,4,7};
        traits.sortKeyValue(values, pos, 2, 7);
        assertArrayEquals(answer, values, 0.000001d);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testReorder() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] pos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        double[] answer = new double[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        double[] reodered = traits.reorder(values, pos);
        assertArrayEquals(answer, reodered, 0.000001d);
    }

    @Test
    public void testCountDistinct() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        traits.sort(values);
        assertEquals(7, traits.countDistinct(values));
        assertEquals(1, traits.countDistinct(new double[]{33}));
        assertEquals(1, traits.countDistinct(new double[]{33, 33}));
        assertEquals(2, traits.countDistinct(new double[]{33, 33, 333}));
    }

    @Test
    public void testFillArray() {
        double[] values = new double[1230];
        Arrays.fill(values, 0L);
        traits.fillArray(3332d, values, 32, 311);
        for (int i = 0; i < values.length; ++i) {
            if (i < 32) {
                assertEquals(0d, values[i], 0.000001d);
            } else if (i < 32 + 311) {
                assertEquals(3332d, values[i], 0.000001d);
            } else {
                assertEquals(0d, values[i], 0.000001d);
            }
        }
    }

    @Test
    public void testGet() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        assertEquals(-234234234d, traits.get(values, 2).doubleValue(), 0.000001d);
        assertEquals(3434d, traits.get(values, 7).doubleValue(), 0.000001d);
    }

    @Test
    public void testSet() {
        double[] values = new double[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        traits.set(values, 3, -545454d);
        traits.set(values, 0, 545454d);
        double[] answer = new double[]{545454,234,-234234234,-545454,4547457,145742,245646,3434};
        assertArrayEquals(answer, values, 0.000001d);
    }

    @Test
    public void testSerialize() {
        double[] values = new double[1233];
        for (int i = 0; i < values.length; ++i) {
            values[i] = i * 76 + i % 40 - 6330;
        }
        int bytes = traits.getSerializedByteSize(values);
        byte[] buf = new byte[bytes * 2];
        {
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
            byteBuffer.put(new byte[3]);
            int written = traits.serializeArray(values, byteBuffer);
            assertEquals(written, bytes);
            assertEquals(3 + bytes, byteBuffer.position());
        }
        {
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 3, buf.length - 3);
            double[] deserialized = traits.deserializeArray(byteBuffer);
            assertEquals(3 + bytes, byteBuffer.position());
            assertArrayEquals(values, deserialized, 0.000001d);
        }
    }

}
