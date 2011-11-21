package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalVarLenReaderVarcharTest {
    private File file;
    private LocalVarLenReader<String> reader;
    
    private final static int VALUE_COUNT = 123;
    
    private static String generateValue (int index) {
        return ("str" + index + "abc");
    }

    @Before
    public void setUp() throws Exception {
        // create the file to test
        file = new File("test/local/stringfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
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
    }
}
