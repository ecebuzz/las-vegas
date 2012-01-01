package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.FixLenValueTraits;

/**
 * Base class of testcases for LocalFixLenReader.
 * Name of this abstract class doesn't end with Test so that our ant script
 * would skip this. 
 */
public abstract class LocalFixLenReaderTestBase<T extends Number & Comparable<T>, AT> {
    protected static VirtualFile file;
    protected FixLenValueTraits<T, AT> traits;
    protected LocalFixLenReader<T, AT> reader;
    
    protected final static int VALUE_COUNT = 123;
    /** deterministically generate a value for index-th entry. */
    protected abstract T generateValue (int index);
    protected abstract FixLenValueTraits<T, AT> createTraits ();
    protected abstract AT createArray (int size);
    protected abstract void setToArray (AT array, int index, T value);
    protected abstract T getFromArray (AT array, int index);

    
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
        file = new LocalVirtualFile("test/local/typedfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        traits = createTraits();
        AT buf = createArray(VALUE_COUNT);
        for (int i = 0; i < VALUE_COUNT; ++i) {
            setToArray(buf, i, generateValue(i));
            if (i % 50 == 0) System.out.println("generated value(" + i + ")=" + generateValue(i) + ":" + generateValue(i).getClass().getName());
        }
        LocalFixLenWriter<T, AT> writer = new LocalFixLenWriter<T, AT>(file, traits);
        writer.writeValues(buf, 0, VALUE_COUNT);
        writer.writeFileFooter();
        writer.flush();
        writer.close();
    }
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        file.delete();
        file = null;
    }

    @Before
    public void setUp() throws Exception {
        initOnce();
        this.traits = createTraits();
        this.reader = new LocalFixLenReader<T, AT>(file, traits);
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
            assertEquals(generateValue(i), value);
        }
        reader.seekToTupleAbsolute(5);
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(5 + i), value);
        }
    }

    @Test
    public void testReadValues() throws IOException {
        AT buf = createArray(15);
        assertEquals(7, reader.readValues(buf, 3, 7));
        for (int i = 0; i < 7; ++i) {
            assertEquals(generateValue(i), getFromArray(buf, 3 + i));
        }
        reader.seekToTupleRelative(20);
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertEquals(generateValue(7 + 20 + i), getFromArray(buf, i));
        }
    }

    @Test
    public void testReadValuesEnd() throws IOException {
        AT buf = createArray(15);
        reader.seekToTupleAbsolute(VALUE_COUNT - 5);
        assertEquals(5, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 5; ++i) {
            assertEquals(generateValue(VALUE_COUNT - 5 + i), getFromArray(buf, i));
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        reader.skipValue();
        reader.skipValue();
        reader.skipValue();
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(3 + i), value);
        }
        reader.skipValue();
        reader.skipValue();
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(3 + 10 + 2 + i), value);
        }
    }

    @Test
    public void testSkipValues() throws IOException {
        reader.skipValues(3);
        for (int i = 0; i < 10; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(3 + i), value);
        }
        reader.skipValues(2);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(3 + 10 + 2 + i), value);
        }
    }

    @Test
    public void testSeekToTupleAbsolute() throws IOException {
        reader.seekToTupleAbsolute(45);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(45 + i), value);
        }
        reader.seekToTupleAbsolute(10);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(10 + i), value);
        }
    }

    @Test
    public void testSeekToTupleRelative() throws IOException {
        reader.seekToTupleRelative(13);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(13 + i), value);
        }
        reader.seekToTupleRelative(-4);
        for (int i = 0; i < 5; ++i) {
            T value = reader.readValue();
            assertEquals(generateValue(13 + 5 - 4 + i), value);
        }
    }
    @Test
    public void testGetTotalTuples() throws IOException {
        assertEquals (VALUE_COUNT, reader.getTotalTuples());
    }
}
