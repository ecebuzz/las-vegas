package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

public class LocalVarLenReaderVarcharTest {
    private VirtualFile file;
    private LocalVarLenReader<String> reader;
    
    private final static int VALUE_COUNT = 123;
    
    private static String generateValue (int index) {
        return ("str" + index + "ab\u6728\u6751c"); // also use some unicode
    }

    @Before
    public void setUp() throws Exception {
        // create the file to test
        file = new LocalVirtualFile("test/local/stringfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        DataOutputStream out = new DataOutputStream(file.getOutputStream());
        for (int i = 0; i < VALUE_COUNT; ++i) {
            byte[] bytes = generateValue(i).getBytes("UTF-8");
            out.writeByte((byte) 4);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        out.flush();
        out.close();

        this.reader = new LocalVarLenReader<String>(file, new AllValueTraits.VarcharValueTraits());
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
            String value = reader.readValue();
            assertEquals(generateValue(i), value);
        }
        reader.skipValues(3);
        for (int i = 0; i < 10; ++i) {
            String value = reader.readValue();
            assertEquals(generateValue(10 + 3 + i), value);
        }
        reader.seekToTupleAbsolute(4);
        for (int i = 0; i < 10; ++i) {
            String value = reader.readValue();
            assertEquals(generateValue(4 + i), value);
        }
        reader.seekToTupleAbsolute(25);
        for (int i = 0; i < 10; ++i) {
            String value = reader.readValue();
            assertEquals(generateValue(25 + i), value);
        }
    }

    @Test
    public void testReadValues() throws IOException {
        String[] buf = new String[15];
        assertEquals(7, reader.readValues(buf, 3, 7));
        for (int i = 0; i < 7; ++i) {
            assertEquals(generateValue(i), buf[3 + i]);
        }
        reader.skipValue();
        reader.skipValue();
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertEquals(generateValue(7 + 2 + i), buf[i]);
        }
        reader.seekToTupleAbsolute(12);
        assertEquals(15, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 15; ++i) {
            assertEquals(generateValue(12 + i), buf[i]);
        }
        
        reader.seekToTupleAbsolute(50);
        assertEquals(10, reader.readValues(buf, 0, 10));
        for (int i = 0; i < 10; ++i) {
            assertEquals(generateValue(50 + i), buf[i]);
        }
    }
    @Test
    public void testReadValuesEnd() throws IOException {
        String[] buf = new String[15];
        reader.seekToTupleAbsolute(VALUE_COUNT - 5);
        assertEquals(5, reader.readValues(buf, 0, 15));
        for (int i = 0; i < 5; ++i) {
            assertEquals(generateValue(VALUE_COUNT - 5 + i), buf[i]);
        }
    }
}
