package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.data.EquiWidthPartitioner;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Testcases for {@link EquiWidthPartitioner}.
 */
public class EquiWidthPartitionerTest {
    private TextFileTupleReader reader;
    private final MiniDataSource dataSource = new MiniSSBLineorder();
    @Before
    public void setUp () throws IOException {
        reader = dataSource.open();
    }
    @After
    public void tearDown () throws IOException {
        reader.close();
        reader = null;
    }
    
    @Test
    public void testOrderKeyNull () throws IOException {
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 0, 1, 400);
        assertEquals(1, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertNull(ranges[0].getEndKey());
    }

    @Test
    public void testOrderKeyAll () throws IOException {
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 0, 100, 10000);
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
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 1, 1, 400);
        assertEquals(1, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertNull(ranges[0].getEndKey());
    }

    @Test
    public void testLineNumberAll () throws IOException {
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 1, 20, 10000);
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
    public void testOrderDateAll () throws IOException {
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 5, 3, 10000);
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
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 6, 1, 400);
        assertEquals(1, ranges.length);
        assertNull(ranges[0].getStartKey());
        assertNull(ranges[0].getEndKey());
    }
    @Test
    public void testOrderPriorityAll () throws IOException {
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 6, 10, 10000);
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
    public void testShipModeAll () throws IOException {
        ValueRange[] ranges = EquiWidthPartitioner.designPartitions(reader, 16, 26, 10000);
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
