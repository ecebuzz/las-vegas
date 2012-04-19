package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.PositionIndex.Pos;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.util.ByteArray;
import edu.brown.lasvegas.util.ChecksumUtil;

/**
 * Testcase for {@link LocalVarLenWriter}.
 * Both byte[] version and String version. Not much different after all!
 */
public class LocalVarLenWriterTest {
    private static String generateValue (int index) {
        return ("str" + index + "ab\u6728\u6751c"); // also use some unicode
    }

    private VirtualFile dataFile;
    private VirtualFile posFile;
    @Before
    public void setUp() throws Exception {
        dataFile = new LocalVirtualFile("test/local/strfile.data");
        if (!dataFile.getParentFile().exists() && !dataFile.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + dataFile.getParentFile().getAbsolutePath());
        }
        dataFile.delete();
        posFile = new LocalVirtualFile("test/local/strfile.data.pos");
        posFile.delete();
    }
    @After
    public void tearDown() throws Exception {
        dataFile.delete();
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
            writer.setCRC32Enabled(true);
            for (int i = 0; i < COUNT; ++i) {
                writer.writeValue(generateValue(i));
            }
            // collect per 100 bytes. data is about 20-30bytes per value. position per 4-5 values.
            writer.writePositionFile(posFile);
            assertTrue (posFile.length() > 0);
            long crc32 = writer.writeFileFooter();
            assertTrue (crc32 != 0);
            writer.flush();
            writer.close();
            long correctCrc32 = ChecksumUtil.getFileCheckSum(dataFile);
            assertEquals (correctCrc32, crc32);
            assertEquals (COUNT, writer.getTupleCount());
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
        
        // test position file indexing (both directly and via LocalVarLenReader)
        LocalVarLenReader<String> readerWithPos = LocalVarLenReader.getInstanceVarchar(dataFile);
        readerWithPos.loadPositionFile(posFile);
        assertEquals (COUNT, readerWithPos.getTotalTuples());
        LocalPosFile positions = new LocalPosFile(posFile);
        assertEquals(COUNT, positions.getTotalTuples());
        final int[] tuplesToSearch = new int[]{1500, 234, 555, 0, 6000, 12344};
        for (int tupleToSearch : tuplesToSearch) {
            readerWithPos.seekToTupleAbsolute(tupleToSearch);
            assertEquals (generateValue(tupleToSearch), readerWithPos.readValue());

            Pos pos = positions.searchPosition(tupleToSearch);
            assertTrue (pos.tuple <= tupleToSearch);
            assertTrue (pos.tuple >= tupleToSearch - 10); // as stated above, shouldn't be off more than 10 values
            LocalVarLenReader<String> reader = LocalVarLenReader.getInstanceVarchar(dataFile);
            reader.getRawReader().seekToByteAbsolute(pos.bytePosition);
            if (tupleToSearch - pos.tuple > 0) {
                reader.skipValues(tupleToSearch - pos.tuple);
            }
            String value = reader.readValue();
            assertEquals (generateValue(tupleToSearch), value);
            reader.close();
        }
        readerWithPos.close();
    }

    @Test
    public void testBytesWriter() throws Exception {
        final int COUNT = 12345;
        {
            LocalVarLenWriter<ByteArray> writer
                = LocalVarLenWriter.getInstanceVarbin(dataFile, 100); // collect often to test the position file easily
            writer.setCRC32Enabled(true);
            for (int i = 0; i < COUNT; ++i) {
                writer.writeValue(new ByteArray(generateValue(i).getBytes("UTF-8")));
            }
            // collect per 100 bytes. data is about 20-30bytes per value. position per 4-5 values.
            writer.writePositionFile(posFile);
            assertTrue (posFile.length() > 0);
            long crc32 = writer.writeFileFooter();
            assertTrue (crc32 != 0);
            writer.flush();
            writer.close();
            long correctCrc32 = ChecksumUtil.getFileCheckSum(dataFile);
            assertEquals (correctCrc32, crc32);
            assertEquals (COUNT, writer.getTupleCount());
        }        
        
        // test sequential scan
        {
            LocalVarLenReader<ByteArray> reader = LocalVarLenReader.getInstanceVarbin(dataFile);
            for (int i = 0; i < COUNT; ++i) {
                if (i % 3 != 0) {
                    ByteArray value = reader.readValue();
                    assertEquals (new ByteArray(generateValue(i).getBytes("UTF-8")), value);
                } else {
                    reader.skipValue();
                }
            }
            reader.close();
        }
        
        // test position file indexing (both directly and via LocalVarLenReader)
        LocalVarLenReader<ByteArray> readerWithPos = LocalVarLenReader.getInstanceVarbin(dataFile);
        readerWithPos.loadPositionFile(posFile);
        assertEquals (COUNT, readerWithPos.getTotalTuples());
        LocalPosFile positions = new LocalPosFile(posFile);
        assertEquals(COUNT, positions.getTotalTuples());
        final int[] tuplesToSearch = new int[]{1500, 234, 555, 0, 6000, 12344};
        for (int tupleToSearch : tuplesToSearch) {
            readerWithPos.seekToTupleAbsolute(tupleToSearch);
            assertEquals (new ByteArray(generateValue(tupleToSearch).getBytes("UTF-8")), readerWithPos.readValue());

            Pos pos = positions.searchPosition(tupleToSearch);
            assertTrue (pos.tuple <= tupleToSearch);
            assertTrue (pos.tuple >= tupleToSearch - 10); // as stated above, shouldn't be off more than 10 values
            LocalVarLenReader<ByteArray> reader = LocalVarLenReader.getInstanceVarbin(dataFile);
            reader.getRawReader().seekToByteAbsolute(pos.bytePosition);
            if (tupleToSearch - pos.tuple > 0) {
                reader.skipValues(tupleToSearch - pos.tuple);
            }
            ByteArray value = reader.readValue();
            assertEquals (new ByteArray(generateValue(tupleToSearch).getBytes("UTF-8")), value);
            reader.close();
        }
        readerWithPos.close();
    }
}
