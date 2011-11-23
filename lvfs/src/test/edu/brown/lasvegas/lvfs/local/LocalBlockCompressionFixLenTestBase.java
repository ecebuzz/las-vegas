package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.FixLenValueTraits;

/**
 * Base class of testcases for {@link LocalBlockCompressionFixLenReader} and {@link LocalBlockCompressionFixLenWriter}.
 * Name of this abstract class doesn't end with Test so that our ant script
 * would skip this. 
 */
public abstract class LocalBlockCompressionFixLenTestBase<T, AT> {
    protected static File file;
    protected FixLenValueTraits<T, AT> traits;
    protected LocalBlockCompressionFixLenReader<T, AT> reader;
    
    protected final static int VALUE_COUNT = 12345;
    /** deterministically generate a value for index-th entry. */
    protected abstract T generateValue (int index);
    protected abstract FixLenValueTraits<T, AT> createTraits ();
    protected abstract AT createArray (int size);
    protected abstract void setToArray (AT array, int index, T value);
    protected abstract T getFromArray (AT array, int index);

    /** override this to change the compression algorithm to use. */
    protected CompressionType getType () {return CompressionType.SNAPPY;}
    
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
        for (int type = 0; type < 2; ++type) {
            // create the file to test
            file = new File("test/local/block_comp.bin");
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
            }
            file.delete();
            traits = createTraits();
            AT buf = createArray(VALUE_COUNT);
            for (int i = 0; i < VALUE_COUNT; ++i) {
                setToArray(buf, i, generateValue(i));
            }
            LocalBlockCompressionFixLenWriter<T, AT> writer = new LocalBlockCompressionFixLenWriter<T, AT>(file, getType(), traits);
            writer.writeValues(buf, 0, VALUE_COUNT);
            writer.writeFileFooter();
            writer.flush();
            writer.close();
        }
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
        this.reader = new LocalBlockCompressionFixLenReader<T, AT>(file, getType(), traits);
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
