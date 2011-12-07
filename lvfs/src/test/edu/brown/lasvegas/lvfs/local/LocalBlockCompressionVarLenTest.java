package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Testcase for {@link LocalBlockCompressionVarLenWriter} and {@link LocalBlockCompressionVarLenReader}.
 * Both byte[] version and String version. Not much different after all!
 */
public class LocalBlockCompressionVarLenTest {
    private static String generateValue (int index) {
        return ("str" + index + "ab\u6728\u6751c"); // also use some unicode
    }

    /** override this to change the compression algorithm to use. */
    protected CompressionType getType () {return CompressionType.SNAPPY;}

    private VirtualFile file;
    @Before
    public void setUp() throws Exception {
        file = new LocalVirtualFile("test/local/strfile.comp.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
    }
    @After
    public void tearDown() throws Exception {
        file.delete();
        file = null;
    }
    
    @Test
    public void testStringWriter() throws Exception {
        final int COUNT = 12345;
        {
            LocalBlockCompressionVarLenWriter<String> writer
                = LocalBlockCompressionVarLenWriter.getInstanceVarchar(file, getType(), 100);
            for (int i = 0; i < COUNT; ++i) {
                writer.writeValue(generateValue(i));
            }
            writer.writeFileFooter();
            writer.flush();
            writer.close();
        }        
        
        // test sequential scan
        {
            LocalBlockCompressionVarLenReader<String> reader = LocalBlockCompressionVarLenReader.getInstanceVarchar(file, getType());
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

        // test seeking
        {
            LocalBlockCompressionVarLenReader<String> reader = LocalBlockCompressionVarLenReader.getInstanceVarchar(file, getType());
            reader.seekToTupleAbsolute(3125);
            assertEquals (generateValue(3125), reader.readValue());
            reader.skipValue();
            assertEquals (generateValue(3127), reader.readValue());
            reader.seekToTupleAbsolute(33);
            assertEquals (generateValue(33), reader.readValue());
            reader.seekToTupleAbsolute(12344);
            assertEquals (generateValue(12344), reader.readValue());
            reader.close();
        }
    }
    

    @Test
    public void testBytesWriter() throws Exception {
        final int COUNT = 12345;
        {
            LocalBlockCompressionVarLenWriter<byte[]> writer
                = LocalBlockCompressionVarLenWriter.getInstanceVarbin(file, getType(), 100);
            for (int i = 0; i < COUNT; ++i) {
                writer.writeValue(generateValue(i).getBytes("UTF-8"));
            }
            writer.writeFileFooter();
            writer.flush();
            writer.close();
        }        
        
        // test sequential scan
        {
            LocalBlockCompressionVarLenReader<byte[]> reader = LocalBlockCompressionVarLenReader.getInstanceVarbin(file, getType());
            for (int i = 0; i < COUNT; ++i) {
                if (i % 3 != 0) {
                    byte[] value = reader.readValue();
                    assertArrayEquals (generateValue(i).getBytes("UTF-8"), value);
                } else {
                    reader.skipValue();
                }
            }
            reader.close();
        }
        // test seeking
        {
            LocalBlockCompressionVarLenReader<byte[]> reader = LocalBlockCompressionVarLenReader.getInstanceVarbin(file, getType());
            reader.seekToTupleAbsolute(3125);
            assertArrayEquals (generateValue(3125).getBytes("UTF-8"), reader.readValue());
            reader.skipValue();
            assertArrayEquals (generateValue(3127).getBytes("UTF-8"), reader.readValue());
            reader.seekToTupleAbsolute(33);
            assertArrayEquals (generateValue(33).getBytes("UTF-8"), reader.readValue());
            reader.seekToTupleAbsolute(12344);
            assertArrayEquals (generateValue(12344).getBytes("UTF-8"), reader.readValue());
            reader.close();
        }
    }
}
