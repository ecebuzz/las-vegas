package edu.brown.lasvegas.traits;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class IntegerValueTraitsTest {
    private IntegerValueTraits traits;
    @Before
    public void setUp () {
        traits = new IntegerValueTraits();
    }
/*   these are well tested in column file reader/writer classes. 
    @Test
    public void testReadValue() {
    }

    @Test
    public void testReadValues() {
    }

    @Test
    public void testWriteValue() {
    }

    @Test
    public void testWriteValues() {
    }
    @Test
    public void testWriteRunengthes() {
    }
*/
    @Test
    public void testGetBitsPerValue() {
        assertEquals (32, traits.getBitsPerValue());
    }


    @Test
    public void testCreateArray() {
        assertEquals (20, traits.createArray(20).length);
    }

    @Test
    public void testength() {
        assertEquals (20, traits.length(traits.createArray(20)));
    }

    @Test
    public void testToArray() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        List<Integer> list = new ArrayList<Integer>();
        for (int val : values) list.add(val);
        int[] array = traits.toArray(list);
        assertArrayEquals(values, array);
    }

    @Test
    public void testBinarySearch() {
        int[] values = new int[123];
        for (int i = 0; i < values.length; ++i) {
            values[i] = i * 23;
        }
        assertEquals (20, traits.binarySearch(values, (int) (20 * 23)));
        assertEquals (- 21 - 1, traits.binarySearch(values, (int) (20 * 23) + 1));
        assertEquals (- 20 - 1, traits.binarySearch(values, (int) (20 * 23) - 1));
        assertEquals (0, traits.binarySearch(values, 0));
        assertEquals (- 1 - 1, traits.binarySearch(values, 1));
        assertEquals (-1, traits.binarySearch(values, -1));
        assertEquals (values.length - 1, traits.binarySearch(values, (int) ((values.length - 1) * 23)));
        assertEquals (- values.length + 1 - 1, traits.binarySearch(values, (int) ((values.length - 1) * 23) - 1));
        assertEquals (- values.length - 1, traits.binarySearch(values, (int) ((values.length - 1) * 23) + 1));
    }

    @Test
    public void testSortIntegerArray() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] answer = new int[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        traits.sort(values);
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSortIntegerArrayIntInt() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] answer = new int[]{234,234,-234234234,4553,145742,245646,4547457,3434};
        traits.sort(values, 2, 7);
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSortKeyValueIntegerArrayIntArray() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] answer = new int[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        traits.sortKeyValue(values, pos);
        assertArrayEquals(answer, values);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testSortKeyValueIntegerArrayIntArrayIntInt() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] answer = new int[]{234,234,-234234234,4553,145742,245646,4547457,3434};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{0,1,2,3,5,6,4,7};
        traits.sortKeyValue(values, pos, 2, 7);
        assertArrayEquals(answer, values);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testReorder() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] pos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        int[] answer = new int[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        int[] reodered = traits.reorder(values, pos);
        assertArrayEquals(answer, reodered);
    }

    @Test
    public void testCountDistinct() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        traits.sort(values);
        assertEquals(7, traits.countDistinct(values));
        assertEquals(1, traits.countDistinct(new int[]{33}));
        assertEquals(1, traits.countDistinct(new int[]{33, 33}));
        assertEquals(2, traits.countDistinct(new int[]{33, 33, 333}));
    }

    @Test
    public void testFillArray() {
        int[] values = new int[1230];
        Arrays.fill(values, 0);
        traits.fillArray(3332, values, 32, 311);
        for (int i = 0; i < values.length; ++i) {
            if (i < 32) {
                assertEquals(0, values[i]);
            } else if (i < 32 + 311) {
                assertEquals(3332, values[i]);
            } else {
                assertEquals(0, values[i]);
            }
        }
    }

    @Test
    public void testGet() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        assertEquals(-234234234, traits.get(values, 2).intValue());
        assertEquals(3434, traits.get(values, 7).intValue());
    }

    @Test
    public void testSet() {
        int[] values = new int[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        traits.set(values, 3, -545454);
        traits.set(values, 0, 545454);
        int[] answer = new int[]{545454,234,-234234234,-545454,4547457,145742,245646,3434};
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSerialize() {
        int[] values = new int[1233];
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
            int[] deserialized = traits.deserializeArray(byteBuffer);
            assertEquals(3 + bytes, byteBuffer.position());
            assertArrayEquals(values, deserialized);
        }
    }


    @Test
    public void testMergeDictionary() {
        int[][] oldDicts = new int[][]{
            new int[]{-5794875, -5432, 0, 34343, 54298234},
            new int[]{-5794875, 3, 54298234},
            new int[]{-5432},
            new int[]{34298234, 44298234, 84298234},
        };
        int[][] conversions = new int[oldDicts.length][];
        int[] newDicts = traits.mergeDictionary(oldDicts, conversions);
        assertArrayEquals(new int[]{-5794875, -5432, 0, 3, 34343, 34298234, 44298234, 54298234, 84298234}, newDicts);
        assertArrayEquals(new int[]{0, 1, 2, 4, 7}, conversions[0]);
        assertArrayEquals(new int[]{0, 3, 7}, conversions[1]);
        assertArrayEquals(new int[]{1}, conversions[2]);
        assertArrayEquals(new int[]{5, 6, 8}, conversions[3]);
    }
}
