package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.util.ChecksumUtil;

/**
 * Testcases for LocalFixLenWriter.
 * Testcases for Readers also use it, so this class only tests a few specific cases. 
 */
public class LocalFixLenWriterTest {
    private VirtualFile file;
    
    private static int generateValue (int index) {
        return (294493 * index) ^ index;
    }

    @Test
    public void testAll() throws Exception {
        // create the file to test
        file = new LocalVirtualFile("test/local/intfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        LocalFixLenWriter<Integer, int[]> writer = LocalFixLenWriter.getInstanceInteger(file);
        writer.setCRC32Enabled(true);
        int[] buf = new int[1 << 14];
        for (int i = 0; i < 25; ++i) {
            for (int j = 0; j < buf.length; ++j) {
                buf[j] = generateValue(i * buf.length + j);
            }
            writer.writeValues(buf, 0, buf.length);
        }
        long crc32 = writer.writeFileFooter();
        assertTrue (crc32 != 0);
        writer.flush();
        writer.close();
        long correctCrc32 = ChecksumUtil.getFileCheckSum(file);
        assertEquals (correctCrc32, crc32);
        assertEquals ((25 << 14) * 4, file.length());

        LocalFixLenReader<Integer, int[]> reader = LocalFixLenReader.getInstanceInteger(file);
        buf = new int[1 << 12]; // uses different buffering
        for (int i = 0; i < 25 * 4; ++i) {
            assertEquals (buf.length, reader.readValues(buf, 0, buf.length));
            for (int j = 0; j < buf.length; ++j) {
                assertEquals (generateValue(i * buf.length + j), buf[j]);
            }
        }
        reader.close();        
        file.delete();
    }

}
