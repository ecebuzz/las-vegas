package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.util.ChecksumUtil;

/**
 * Base class of testcases for {@link LocalRLEReader} and {@link LocalRLEWriter}.
 * Name of this abstract class doesn't end with Test so that our ant script
 * would skip this. 
 */
public abstract class LocalRLETestBase<T extends Comparable<T>, AT> {
    protected static VirtualFile file, pos;
    protected ValueTraits<T, AT> traits;
    protected LocalRLEReader<T, AT> reader;
    
    protected final static int VALUE_COUNT = 12345;
    /** deterministically generate a value for index-th entry. */
    protected abstract T generateValue (int index);
    protected abstract ValueTraits<T, AT> createTraits ();
    protected abstract AT createArray (int size);
    protected abstract void setToArray (AT array, int index, T value);
    protected abstract T getFromArray (AT array, int index);
    /** override if T is an array object (only varbin). */
    protected void assertEqualsT (T a, T b) {
        assertEquals (a, b);
    }
    
    private static HashSet<Class<?>> inittedClasses = new HashSet<Class<?>>();
    /**
     * "BeforeClass" has to be static method, but this class needs an instance to initialize.
     * So, each setUp() calls this method to do the one-time initialization.
     */
    private void initOnce() throws Exception {
        if (inittedClasses.contains(getClass())) {
            return;
        }
        inittedClasses.add(getClass());
        // create the file to test
        file = new LocalVirtualFile("test/local/block_comp.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        pos = new LocalVirtualFile("test/local/block_comp.bin.pos");
        pos.delete();
        traits = createTraits();
        AT buf = createArray(VALUE_COUNT);
        for (int i = 0; i < VALUE_COUNT; ++i) {
            setToArray(buf, i, generateValue(i));
        }
        LocalRLEWriter<T, AT> writer = new LocalRLEWriter<T, AT>(file, traits);
        writer.setCRC32Enabled(true);
        writer.writeValues(buf, 0, VALUE_COUNT);
        long crc32 = writer.writeFileFooter();
        assertTrue (crc32 != 0);
        writer.flush();
        writer.close();
        long correctCrc32 = ChecksumUtil.getFileCheckSum(file);
        assertEquals (correctCrc32, crc32);
        writer.writePositionFile(pos);
        runCount = writer.getRunCount();
    }
    protected int runCount;
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        file.delete();
        file = null;
        pos.delete();
        pos = null;
    }

    @Before
    public void setUp() throws Exception {
        initOnce();
        this.traits = createTraits();
        this.reader = new LocalRLEReader<T, AT>(file, traits);
        this.reader.loadPositionFile(pos);
    }
    @After
    public void tearDown() throws Exception {
        this.traits = null;
        this.reader.close();
        this.reader = null;
    }

    @Test
    public void testReadValue() throws IOException {
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(i), value);
        }
        reader.seekToTupleAbsolute(5);
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(5 + i), value);
        }
    }

    @Test
    public void testReadValues() throws IOException {
        AT buf = createArray(15);
        assertEquals(7, reader.readValues(buf, 3, 7));
        for (int i = 0; i < 7; ++i) {
            assertEqualsT(generateValue(i), getFromArray(buf, 3 + i));
        }
        reader.seekToTupleAbsolute(20);
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertEqualsT(generateValue(20 + i), getFromArray(buf, i));
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        reader.skipValue();
        reader.skipValue();
        reader.skipValue();
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(3 + i), value);
        }
        reader.skipValue();
        reader.skipValue();
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(3 + 10 + 2 + i), value);
        }
    }

    @Test
    public void testSkipValues() throws IOException {
        reader.skipValues(3);
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(3 + i), value);
        }
        reader.skipValues(2);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(3 + 10 + 2 + i), value);
        }
    }

    @Test
    public void testSeekToTupleAbsolute() throws IOException {
        reader.seekToTupleAbsolute(45);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(45 + i), value);
        }
        reader.seekToTupleAbsolute(10);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEqualsT(generateValue(10 + i), value);
        }
    }

    @Test
    public void testGetTotalTuples() throws IOException {
        assertEquals (VALUE_COUNT, reader.getTotalTuples());
    }

}
