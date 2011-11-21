package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Base testcase for both reader and writer.
 * Only difference is how to create the data file (setUpBeforeClass).
 * Name of this abstract class doesn't end with Test so that our ant script
 * would skip this. 
 */
public abstract class LocalRawFileTestBase {
    protected static File file;
    
    @Before
    public void setUp() throws Exception {
        reader = new LocalRawFileReader(file);
    }

    @After
    public void tearDown() throws Exception {
        reader.close();
        reader = null;
    }
    
    private LocalRawFileReader reader;

    @Test
    public void testSeek() throws IOException {
        reader.seekToByteAbsolute(7);
        assertEquals((short)-4520, reader.readShort()); // now 9
        reader.seekToByteRelative(10); // now 19
        assertEquals(0xD02E908A2490F939L, reader.readLong()); // now 27
        reader.seekToByteRelative(-18); // now 9
        assertEquals((short)31001, reader.readShort()); // now 11
        reader.seekToByteAbsolute(0);
        assertEquals((byte)-120, reader.readByte());
    }

    @Test
    public void testReadBytes() throws IOException {
        byte[] buf = new byte[10];
        Arrays.fill(buf, (byte) 0);
        int read = reader.readBytes(buf, 5, 3);
        assertEquals(3, read);
        for (int i = 0; i < 10; ++i) {
            switch(i) {
                case 5: assertEquals((byte) -120, buf[i]); break;
                case 7: assertEquals((byte) 40, buf[i]); break;
                default:
                    assertEquals((byte) 0, buf[i]); break;
            }
        }
    }

    @Test
    public void testReadByte() throws IOException {
        assertEquals((byte) -120, reader.readByte());
        assertEquals((byte) 0, reader.readByte());
        assertEquals((byte) 40, reader.readByte());
    }

    @Test
    public void testReadBoolean() throws IOException {
        reader.seekToByteAbsolute(3);
        assertEquals(false, reader.readBoolean());
        assertEquals(true, reader.readBoolean());
    }

    @Test
    public void testReadShort() throws IOException {
        reader.seekToByteAbsolute(7);
        assertEquals((short)-4520, reader.readShort());
        assertEquals((short)31001, reader.readShort());
    }

    @Test
    public void testReadInt() throws IOException {
        reader.seekToByteAbsolute(11);
        assertEquals(0x2490F939, reader.readInt());
        assertEquals(0xB490F979, reader.readInt());
    }

    @Test
    public void testReadLong() throws IOException {
        reader.seekToByteAbsolute(19);
        assertEquals(0xD02E908A2490F939L, reader.readLong());
        assertEquals(0x102E908A2490F9EAL, reader.readLong());
    }

    @Test
    public void testReadFloat() throws IOException {
        reader.seekToByteAbsolute(35);
        assertEquals(0.00345f, reader.readFloat(), 0.0000001f);
        assertEquals(-98724957.34f, reader.readFloat(), 0.01f);
    }

    @Test
    public void testReadDouble() throws IOException {
        reader.seekToByteAbsolute(43);
        assertEquals(0.00000000345d, reader.readDouble(), 0.0000000000001d);
        assertEquals(-9872495734907234324.09d, reader.readDouble(), 0.0001d);
    }

}
