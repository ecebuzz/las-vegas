package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.AllValueTraits;

public class LocalVarLenReaderVarbinTest {
    private File file;
    private LocalVarLenReader<byte[]> reader;
    
    private final static int VALUE_COUNT = 123;
    
    private static byte[] generateValue (int index) {
        byte[] array = new byte[(index % 32) + 4];
        for (int i = 0; i < array.length; ++i) {
            array[i] = (byte) (index ^ i);
        }
        return array;
    }

    @Before
    public void setUp() throws Exception {
        // create the file to test
        file = new File("test/local/binfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        for (int i = 0; i < VALUE_COUNT; ++i) {
            byte[] bytes = generateValue(i);
            out.writeByte((byte) 4);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        out.flush();
        out.close();

        this.reader = new LocalVarLenReader<byte[]>(file, new AllValueTraits.VarbinValueTraits());
    }
    @After
    public void tearDown() throws Exception {
        this.reader.close();
        this.reader = null;
        file.delete();
        file = null;
    }

    @Test
    public void testReadValue() throws IOException {
        for (int i = 0; i < 10; ++i) {
            byte[] value = reader.readValue();
            assertArrayEquals(generateValue(i), value);
        }
        reader.skipValues(3);
        for (int i = 0; i < 10; ++i) {
            byte[] value = reader.readValue();
            assertArrayEquals(generateValue(10 + 3 + i), value);
        }
        reader.seekToTupleAbsolute(30);
        for (int i = 0; i < 7; ++i) {
            byte[] value = reader.readValue();
            assertArrayEquals(generateValue(30 + i), value);
        }
        reader.seekToTupleAbsolute(4);
        for (int i = 0; i < 7; ++i) {
            byte[] value = reader.readValue();
            assertArrayEquals(generateValue(4 + i), value);
        }
    }

    @Test
    public void testReadValues() throws IOException {
        byte[][] buf = new byte[15][];
        assertEquals(7, reader.readValues(buf, 3, 7));
        for (int i = 0; i < 7; ++i) {
            assertArrayEquals(generateValue(i), buf[3 + i]);
        }
        reader.skipValue();
        reader.skipValue();
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertArrayEquals(generateValue(7 + 2 + i), buf[i]);
        }

        reader.seekToTupleAbsolute(51);
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertArrayEquals(generateValue(51 + i), buf[i]);
        }

        reader.seekToTupleAbsolute(20);
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertArrayEquals(generateValue(20 + i), buf[i]);
        }
    }
    @Test
    public void testReadValuesEnd() throws IOException {
        byte[][] buf = new byte[15][];
        reader.seekToTupleAbsolute(VALUE_COUNT - 5);
        assertEquals(5, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 5; ++i) {
            assertArrayEquals(generateValue(VALUE_COUNT - 5 + i), buf[i]);
        }
    }
}
