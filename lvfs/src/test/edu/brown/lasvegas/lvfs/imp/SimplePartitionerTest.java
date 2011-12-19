package edu.brown.lasvegas.lvfs.imp;

import static org.junit.Assert.*;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcases for {@link SimplePartitioner}.
 */
public class SimplePartitionerTest {
    private TextFileTableReader reader;
    @Before
    public void setUp () throws IOException {
        reader = MiniLineorder.open();
    }
    @After
    public void tearDown () throws IOException {
        reader.close();
        reader = null;
    }
    
    @Test
    public void testOrderKeyNull () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 0, 1, 400);
        assertEquals(1, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertNull(ranges[0].getEndKey());
    }
    @Test
    public void testOrderKey () throws IOException {
        // only first few lows and last few lows
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 0, 4, 400);
        assertEquals(5, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertEquals(1, ranges[0].getEndKey());
        assertEquals(1, ranges[1].getStartKey());
        assertEquals(9, ranges[1].getEndKey());
        assertEquals(9, ranges[2].getStartKey());
        assertEquals(18, ranges[2].getEndKey());
        assertEquals(18, ranges[3].getStartKey());
        assertEquals(27, ranges[3].getEndKey());
        assertEquals(27, ranges[4].getStartKey());
        assertNull(ranges[4].getEndKey());
    }

    @Test
    public void testOrderKeyAll () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 0, 100, 10000);
        assertEquals(36, ranges.length);
        for (int i = 0; i < ranges.length; ++i) {
            if (i == 0) {
                assertNull(ranges[i].getStartKey());
            } else {
                assertEquals(i, ranges[i].getStartKey());
            }
            if (i == ranges.length - 1) {
                assertNull(ranges[i].getEndKey());
            } else {
                assertEquals((i + 1), ranges[i].getEndKey());
            }
        }
    }

    @Test
    public void testLineNumberNull () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 1, 1, 400);
        assertEquals(1, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertNull(ranges[0].getEndKey());
    }
    @Test
    public void testLineNumber () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 1, 20, 400);
        assertEquals(6, ranges.length);
        for (int i = 0; i < ranges.length; ++i) {
            if (i == 0) {
                assertNull(ranges[i].getStartKey());
            } else {
                assertEquals((byte) i, ranges[i].getStartKey());
            }
            if (i == ranges.length - 1) {
                assertNull(ranges[i].getEndKey());
            } else {
                assertEquals((byte) (i + 1), ranges[i].getEndKey());
            }
        }
    }

    @Test
    public void testLineNumberAll () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 1, 20, 10000);
        assertEquals(7, ranges.length);
        for (int i = 0; i < ranges.length; ++i) {
            if (i == 0) {
                assertNull(ranges[i].getStartKey());
            } else {
                assertEquals((byte) i, ranges[i].getStartKey());
            }
            if (i == ranges.length - 1) {
                assertNull(ranges[i].getEndKey());
            } else {
                assertEquals((byte) (i + 1), ranges[i].getEndKey());
            }
        }
    }

    @Test
    public void testOrderDate () throws IOException {
        // only first few lows and last few lows
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 5, 4, 400);
        assertEquals(5, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertEquals(19930111, ranges[0].getEndKey());
        assertEquals(19930111, ranges[1].getStartKey());
        assertEquals(19937615, ranges[1].getEndKey()); // this value doesn't make sense, but uniform partitioner can't be that smart
        assertEquals(19937615, ranges[2].getStartKey());
        assertEquals(19945120, ranges[2].getEndKey());
        assertEquals(19945120, ranges[3].getStartKey());
        assertEquals(19952625, ranges[3].getEndKey());
        assertEquals(19952625, ranges[4].getStartKey());
        assertNull(ranges[4].getEndKey());
    }

    @Test
    public void testOrderDateAll () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 5, 3, 10000);
        assertEquals(4, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertEquals(19930111, ranges[0].getEndKey());
        assertEquals(19930111, ranges[1].getStartKey());
        assertEquals(19943717, ranges[1].getEndKey());
        assertEquals(19943717, ranges[2].getStartKey());
        assertEquals(19957323, ranges[2].getEndKey());
        assertEquals(19957323, ranges[3].getStartKey());
        assertNull(ranges[3].getEndKey());
    }

    @Test
    public void testOrderPriorityNull () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 6, 1, 400);
        assertEquals(1, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertNull(ranges[0].getEndKey());
    }
    @Test
    public void testOrderPriority () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 6, 10, 400);
        assertEquals(2, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertEquals("2", ranges[0].getEndKey());
        assertEquals("2", ranges[1].getStartKey());
        assertNull(ranges[1].getEndKey());
    }

    @Test
    public void testOrderPriorityAll () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 6, 10, 10000);
        assertEquals(5, ranges.length);
        for (int i = 0; i < ranges.length; ++i) {
            if (i == 0) {
                assertNull(ranges[i].getStartKey());
            } else {
                assertEquals("" + i, ranges[i].getStartKey());
            }
            if (i == ranges.length - 1) {
                assertNull(ranges[i].getEndKey());
            } else {
                assertEquals("" + (i + 1), ranges[i].getEndKey());
            }
        }
    }

    @Test
    public void testShipMode () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 16, 3, 200);
        assertEquals(4, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertEquals("M", ranges[0].getEndKey());
        assertEquals("M", ranges[1].getStartKey());
        assertEquals("O", ranges[1].getEndKey());
        assertEquals("O", ranges[2].getStartKey());
        assertEquals("Q", ranges[2].getEndKey());
        assertEquals("Q", ranges[3].getStartKey());
        assertNull(ranges[3].getEndKey());
    }

    @Test
    public void testShipModeAll () throws IOException {
        ValueRange<?>[] ranges = SimplePartitioner.designPartitions(reader, reader, 16, 26, 10000);
        assertEquals(20, ranges.length);
        for (int i = 0; i < ranges.length; ++i) {
            if (i == 0) {
                assertNull(ranges[i].getStartKey());
            } else {
                assertEquals(String.valueOf((char) ('A' + i - 1)), ranges[i].getStartKey());
            }
            if (i == ranges.length - 1) {
                assertNull(ranges[i].getEndKey());
            } else {
                assertEquals(String.valueOf((char) ('A' + i)), ranges[i].getEndKey());
            }
        }
    }
}
