package edu.brown.lasvegas.traits;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class BigintValueTraitsTest {
    private BigintValueTraits traits;
    @Before
    public void setUp () {
        traits = new BigintValueTraits();
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
    public void testWriteRunLengthes() {
    }
*/
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
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        List<Long> list = new ArrayList<Long>();
        for (long val : values) list.add(val);
        long[] array = traits.toArray(list);
        assertArrayEquals(values, array);
    }

    @Test
    public void testBinarySearch() {
        long[] values = new long[123];
        for (int i = 0; i < values.length; ++i) {
            values[i] = i * 23;
        }
        assertEquals (20, traits.binarySearch(values, (long) (20 * 23)));
        assertEquals (- 21 - 1, traits.binarySearch(values, (long) (20 * 23) + 1));
        assertEquals (- 20 - 1, traits.binarySearch(values, (long) (20 * 23) - 1));
        assertEquals (0, traits.binarySearch(values, 0L));
        assertEquals (- 1 - 1, traits.binarySearch(values, 1L));
        assertEquals (-1, traits.binarySearch(values, -1L));
        assertEquals (values.length - 1, traits.binarySearch(values, (long) ((values.length - 1) * 23)));
        assertEquals (- values.length + 1 - 1, traits.binarySearch(values, (long) ((values.length - 1) * 23) - 1));
        assertEquals (- values.length - 1, traits.binarySearch(values, (long) ((values.length - 1) * 23) + 1));
    }

    @Test
    public void testSortLongArray() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        long[] answer = new long[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        traits.sort(values);
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSortLongArrayIntInt() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        long[] answer = new long[]{234,234,-234234234,4553,145742,245646,4547457,3434};
        traits.sort(values, 2, 7);
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSortKeyValueLongArrayIntArray() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        long[] answer = new long[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        traits.sortKeyValue(values, pos);
        assertArrayEquals(answer, values);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testSortKeyValueLongArrayIntArrayIntInt() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        long[] answer = new long[]{234,234,-234234234,4553,145742,245646,4547457,3434};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{0,1,2,3,5,6,4,7};
        traits.sortKeyValue(values, pos, 2, 7);
        assertArrayEquals(answer, values);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testReorder() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        int[] pos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        long[] answer = new long[]{-234234234,234,234,3434,4553,145742,245646,4547457};
        long[] reodered = traits.reorder(values, pos);
        assertArrayEquals(answer, reodered);
    }

    @Test
    public void testCountDistinct() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        traits.sort(values);
        assertEquals(7, traits.countDistinct(values));
        assertEquals(1, traits.countDistinct(new long[]{33}));
        assertEquals(1, traits.countDistinct(new long[]{33, 33}));
        assertEquals(2, traits.countDistinct(new long[]{33, 33, 333}));
    }

    @Test
    public void testFillArray() {
        long[] values = new long[1230];
        Arrays.fill(values, 0L);
        traits.fillArray(3332L, values, 32, 311);
        for (int i = 0; i < values.length; ++i) {
            if (i < 32) {
                assertEquals(0, values[i]);
            } else if (i < 32 + 311) {
                assertEquals(3332L, values[i]);
            } else {
                assertEquals(0, values[i]);
            }
        }
    }

    @Test
    public void testGet() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        assertEquals(-234234234L, traits.get(values, 2).longValue());
        assertEquals(3434, traits.get(values, 7).longValue());
    }

    @Test
    public void testSet() {
        long[] values = new long[]{234,234,-234234234,4553,4547457,145742,245646,3434};
        traits.set(values, 3, -545454L);
        traits.set(values, 0, 545454L);
        long[] answer = new long[]{545454,234,-234234234,-545454,4547457,145742,245646,3434};
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSerialize() {
        long[] values = new long[1233];
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
            long[] deserialized = traits.deserializeArray(byteBuffer);
            assertEquals(3 + bytes, byteBuffer.position());
            assertArrayEquals(values, deserialized);
        }
    }


    @Test
    public void testMergeDictionary() {
        long[][] oldDicts = new long[][]{
            new long[]{-5794875L, -5432L, 0L, 34343L, 54298234L},
            new long[]{-5794875L, 3L, 54298234L},
            new long[]{-5432L},
            new long[]{34298234L, 44298234L, 84298234L},
        };
        int[][] conversions = new int[oldDicts.length][];
        long[] newDicts = traits.mergeDictionary(oldDicts, conversions);
        assertArrayEquals(new long[]{-5794875L, -5432L, 0L, 3L, 34343L, 34298234L, 44298234L, 54298234L, 84298234L}, newDicts);
        assertArrayEquals(new int[]{0, 1, 2, 4, 7}, conversions[0]);
        assertArrayEquals(new int[]{0, 3, 7}, conversions[1]);
        assertArrayEquals(new int[]{1}, conversions[2]);
        assertArrayEquals(new int[]{5, 6, 8}, conversions[3]);
    }
}
