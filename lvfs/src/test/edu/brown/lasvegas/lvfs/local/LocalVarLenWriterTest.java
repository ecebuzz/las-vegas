package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.local.LocalPosFile.Pos;

/**
 * Testcase for {@link LocalVarLenWriter}.
 * Both byte[] version and String version. Not much different after all!
 */
public class LocalVarLenWriterTest {
    private static String generateValue (int index) {
        return ("str" + index + "ab\u6728\u6751c"); // also use some unicode
    }

    private File dataFile;
    private File posFile;
    @Before
    public void setUp() throws Exception {
        dataFile = new File("test/local/strfile.data");
        if (!dataFile.getParentFile().exists() && !dataFile.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + dataFile.getParentFile().getAbsolutePath());
        }
        dataFile.delete();
        posFile = new File("test/local/strfile.data.pos");
        posFile.delete();
    }
    @After
    public void tearDown() throws Exception {
        //dataFile.delete();
        dataFile = null;
        posFile.delete();
        posFile = null;
    }
    
    @Test
    public void testStringWriter() throws Exception {
        final int COUNT = 12345;
        {
            LocalVarLenWriter<String> writer
                = LocalVarLenWriter.getInstanceVarchar(dataFile, 100); // collect often to test the position file easily
            for (int i = 0; i < COUNT; ++i) {
                writer.writeValue(generateValue(i));
            }
            // collect per 100 bytes. data is about 20-30bytes per value. position per 4-5 values.
            writer.writePositionFile(posFile);
            assertTrue (posFile.length() > 0);
            writer.writeFileFooter();
            writer.flush();
            writer.close();
        }        
        
        // test sequential scan
        {
            LocalVarLenReader<String> reader = LocalVarLenReader.getInstanceVarchar(dataFile);
            for (int i = 0; i < COUNT; ++i) {
                if (i % 3 != 0) {
                    String value = reader.readValue();
                    assertEquals (generateValue(i), value);
                } else {
                    reader.skipValue();
                }
            }
            reader.close();
        }
        
        // test position file indexing
        LocalPosFile positions = new LocalPosFile(posFile);
        assertEquals(COUNT, positions.getTotalTuples());
        final long[] tuplesToSearch = new long[]{1500, 234, 555, 0, 6000, 12344};
        for (long tupleToSearch : tuplesToSearch) {
            Pos pos = positions.searchPosition(tupleToSearch);
            assertTrue (pos.tuple <= tupleToSearch);
            assertTrue (pos.tuple >= tupleToSearch - 10); // as stated above, shouldn't be off more than 10 values
            LocalVarLenReader<String> reader = LocalVarLenReader.getInstanceVarchar(dataFile);
            reader.seekToByteAbsolute(pos.bytePosition);
            if (tupleToSearch - pos.tuple > 0) {
                reader.skipValues((int) (tupleToSearch - pos.tuple));
            }
            String value = reader.readValue();
            assertEquals (generateValue((int) tupleToSearch), value);
            reader.close();
        }
    }
}
