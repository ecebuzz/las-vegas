package edu.brown.lasvegas.traits;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class VarcharValueTraitsTest {
    private VarcharValueTraits traits;
    @Before
    public void setUp () {
        traits = new VarcharValueTraits();
    }

    @Test
    public void testCreateArray() {
        assertEquals (20, traits.createArray(20).length);
    }

    @Test
    public void testLength() {
        assertEquals (20, traits.length(traits.createArray(20)));
    }
    
    private String format (long val) {
        assert (val >= -1000000000000L);
        return "str" + String.format("%13d", 1000000000000L + val);
    }

    @Test
    public void testToArray() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        List<String> list = new ArrayList<String>();
        for (String val : values) list.add(val);
        String[] array = traits.toArray(list);
        assertArrayEquals(values, array);
    }

    @Test
    public void testBinarySearch() {
        String[] values = new String[123];
        for (int i = 0; i < values.length; ++i) {
            values[i] = format(i * 23);
        }
        assertEquals (20, traits.binarySearch(values, format(20 * 23)));
        assertEquals (- 21 - 1, traits.binarySearch(values, format((20 * 23) + 1)));
        assertEquals (- 20 - 1, traits.binarySearch(values, format((20 * 23) - 1)));
        assertEquals (0, traits.binarySearch(values, format(0L)));
        assertEquals (- 1 - 1, traits.binarySearch(values, format(1L)));
        assertEquals (-1, traits.binarySearch(values, format(-1L)));
        assertEquals (values.length - 1, traits.binarySearch(values, format(((values.length - 1) * 23))));
        assertEquals (- values.length + 1 - 1, traits.binarySearch(values, format(((values.length - 1) * 23) - 1)));
        assertEquals (- values.length - 1, traits.binarySearch(values, format(((values.length - 1) * 23) + 1)));
    }

    @Test
    public void testSortStringArray() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        String[] answer = new String[]{format(-234234234),format(234),format(234),format(3434),format(4553),format(145742),format(245646),format(4547457)};
        traits.sort(values);
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSortStringArrayIntInt() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        String[] answer = new String[]{format(234),format(234),format(-234234234),format(4553),format(145742),format(245646),format(4547457),format(3434)};
        traits.sort(values, 2, 7);
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSortKeyValueStringArrayIntArray() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        String[] answer = new String[]{format(-234234234),format(234),format(234),format(3434),format(4553),format(145742),format(245646),format(4547457)};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        traits.sortKeyValue(values, pos);
        assertArrayEquals(answer, values);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testSortKeyValueStringArrayIntArrayIntInt() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        String[] answer = new String[]{format(234),format(234),format(-234234234),format(4553),format(145742),format(245646),format(4547457),format(3434)};
        int[] pos = new int[]{0,1,2,3,4,5,6,7};
        int[] answerPos = new int[]{0,1,2,3,5,6,4,7};
        traits.sortKeyValue(values, pos, 2, 7);
        assertArrayEquals(answer, values);
        assertArrayEquals(answerPos, pos);
    }

    @Test
    public void testReorder() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        int[] pos = new int[]{2, 0, 1, 7, 3, 5, 6, 4};
        String[] answer = new String[]{format(-234234234),format(234),format(234),format(3434),format(4553),format(145742),format(245646),format(4547457)};
        String[] reodered = traits.reorder(values, pos);
        assertArrayEquals(answer, reodered);
    }

    @Test
    public void testCountDistinct() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        traits.sort(values);
        assertEquals(7, traits.countDistinct(values));
        assertEquals(1, traits.countDistinct(new String[]{format(33)}));
        assertEquals(1, traits.countDistinct(new String[]{format(33), format(33)}));
        assertEquals(2, traits.countDistinct(new String[]{format(33), format(33), format(333)}));
    }

    @Test
    public void testFillArray() {
        String[] values = new String[1230];
        Arrays.fill(values, format(0L));
        traits.fillArray(format(3332L), values, 32, 311);
        for (int i = 0; i < values.length; ++i) {
            if (i < 32) {
                assertEquals(format(0), values[i]);
            } else if (i < 32 + 311) {
                assertEquals(format(3332L), values[i]);
            } else {
                assertEquals(format(0), values[i]);
            }
        }
    }

    @Test
    public void testGet() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        assertEquals(format(-234234234L), traits.get(values, 2));
        assertEquals(format(3434), traits.get(values, 7));
    }

    @Test
    public void testSet() {
        String[] values = new String[]{format(234),format(234),format(-234234234),format(4553),format(4547457),format(145742),format(245646),format(3434)};
        traits.set(values, 3, format(-545454L));
        traits.set(values, 0, format(545454L));
        String[] answer = new String[]{format(545454L),format(234),format(-234234234),format(-545454L),format(4547457),format(145742),format(245646),format(3434)};
        assertArrayEquals(answer, values);
    }

    @Test
    public void testSerialize() throws IOException {
        String[] values = new String[1233];
        for (int i = 0; i < values.length; ++i) {
            values[i] = format(i * 76 + i % 40 - 6330);
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
            String[] deserialized = traits.deserializeArray(byteBuffer);
            assertEquals(3 + bytes, byteBuffer.position());
            assertArrayEquals(values, deserialized);
        }
    }

    @Test
    public void testMergeDictionary() {
        String[][] oldDicts = new String[][]{
            new String[]{format(-5794875L), format(-5432L), format(0L), format(34343L), format(54298234L)},
            new String[]{format(-5794875L), format(3L), format(54298234L)},
            new String[]{format(-5432L)},
            new String[]{format(34298234L), format(44298234L), format(84298234L)},
        };
        int[][] conversions = new int[oldDicts.length][];
        String[] newDicts = traits.mergeDictionary(oldDicts, conversions);
        assertArrayEquals(new String[]{format(-5794875L), format(-5432L), format(0L), format(3L), format(34343L), format(34298234L), format(44298234L), format(54298234L), format(84298234L)}, newDicts);
        assertArrayEquals(new int[]{0, 1, 2, 4, 7}, conversions[0]);
        assertArrayEquals(new int[]{0, 3, 7}, conversions[1]);
        assertArrayEquals(new int[]{1}, conversions[2]);
        assertArrayEquals(new int[]{5, 6, 8}, conversions[3]);
    }
}
