package edu.brown.lasvegas.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

/**
 * Testcases for {@link KeyValueArrays}.
 */
public class KeyValueArraysTest {
    private Random rand;
    private final static int LEN = 332;
    private final static int TIE_PER = 20;
    private final static int TIE_MOD = 3;
    private final static int RANGE_FROM = 89;
    private final static int RANGE_TO = RANGE_FROM + 132;
    private int[] values;
    @Before
    public void setUp () {
        rand = new Random (12345L); // fixed seed
        values = new int[LEN];
        for (int i = 0; i < LEN; ++i) {
            values[i] = i;
        }
    }
    
    @Test
    public void testSortLong() {
        long[] keys = new long[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextLong();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        long[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != 0) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortLongRange () {
        long[] keys = new long[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextLong();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        long[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != RANGE_FROM) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortInt() {
        int[] keys = new int[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        int[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != 0) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortIntRange () {
        int[] keys = new int[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        int[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != RANGE_FROM) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }


    @Test
    public void testSortShort() {
        short[] keys = new short[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = (short) rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        short[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != 0) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortShortRange () {
        short[] keys = new short[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = (short) rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        short[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != RANGE_FROM) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortByte() {
        byte[] keys = new byte[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = (byte) rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        byte[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != 0) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortByteRange () {
        byte[] keys = new byte[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = (byte) rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        byte[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != RANGE_FROM) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortChar() {
        char[] keys = new char[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = (char) rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        char[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != 0) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortCharRange () {
        char[] keys = new char[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = (char) rand.nextInt();
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        char[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != RANGE_FROM) {
                assertTrue(keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortDouble() {
        double[] keys = new double[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextDouble();
            if (i % 79 == 0) keys[i] = Double.NaN;
            if (i % 53 == 0) keys[i] = Double.NEGATIVE_INFINITY;
            if (i % 187 == 0) keys[i] = Double.POSITIVE_INFINITY;
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        double[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i], 0);
            if (i != 0) {
                assertTrue(Double.isNaN(keys[i]) || keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortDoubleRange () {
        double[] keys = new double[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextDouble();
            if (i % 79 == 0) keys[i] = Double.NaN;
            if (i % 53 == 0) keys[i] = Double.NEGATIVE_INFINITY;
            if (i % 187 == 0) keys[i] = Double.POSITIVE_INFINITY;
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        double[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i], 0);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i], 0);
            if (i != RANGE_FROM) {
                assertTrue(Double.isNaN(keys[i]) || keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i], 0);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortFloat() {
        float[] keys = new float[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextFloat();
            if (i % 79 == 0) keys[i] = Float.NaN;
            if (i % 53 == 0) keys[i] = Float.NEGATIVE_INFINITY;
            if (i % 187 == 0) keys[i] = Float.POSITIVE_INFINITY;
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        float[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i], 0);
            if (i != 0) {
                assertTrue(Float.isNaN(keys[i]) || keys[i] >= keys[i - 1]);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortFloatRange () {
        float[] keys = new float[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = rand.nextFloat();
            if (i % 79 == 0) keys[i] = Float.NaN;
            if (i % 53 == 0) keys[i] = Float.NEGATIVE_INFINITY;
            if (i % 187 == 0) keys[i] = Float.POSITIVE_INFINITY;
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        float[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i], 0);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i], 0);
            if (i != RANGE_FROM) {
                assertTrue(Float.isNaN(keys[i]) || keys[i] >= keys[i - 1]);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i], 0);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }


    @Test
    public void testSortString() {
        String[] keys = new String[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = String.format("abc%10d", Math.abs(rand.nextInt()));
            if (i % 79 == 0) keys[i] = null;
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        String[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < LEN; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != 0) {
                assertTrue(keys[i - 1] == null || keys[i].compareTo(keys[i - 1]) >= 0);
            }
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }

    @Test
    public void testSortStringRange () {
        String[] keys = new String[LEN];
        for (int i = 0; i < LEN; ++i) {
            keys[i] = String.format("abc%10d", Math.abs(rand.nextInt()));
            if (i % 79 == 0) keys[i] = null;
            if (i % TIE_PER == TIE_MOD) keys[i] = keys[i - TIE_MOD]; // to introduce a few 'ties'
        }
        String[] orgKeys = keys.clone();
        
        KeyValueArrays.sort(keys, values, RANGE_FROM, RANGE_TO);
        
        boolean[] found = new boolean[LEN];
        Arrays.fill(found, false);
        for (int i = 0; i < RANGE_FROM; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        for (int i = RANGE_FROM; i < RANGE_TO; ++i) {
            int v = values[i];
            assertFalse (found[v]);
            found[v] = true;
            assertEquals (orgKeys[v], keys[i]);
            if (i != RANGE_FROM) {
                assertTrue(keys[i - 1] == null || keys[i].compareTo(keys[i - 1]) >= 0);
            }
        }
        for (int i = RANGE_TO; i < LEN; ++i) {
            assertEquals (orgKeys[i], keys[i]);
            int v = values[i];
            assertEquals (i, v);
            assertFalse (found[v]);
            found[v] = true;
        }
        
        for (int i = 0; i < LEN; ++i) {
            assertTrue (found[i]);
        }
    }
}
