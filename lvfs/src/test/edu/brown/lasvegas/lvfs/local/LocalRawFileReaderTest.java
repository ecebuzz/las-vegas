package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testcase for {@link LocalRawFileReader}.
 */
public class LocalRawFileReaderTest {
    private static File file;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // create the file to test
        file = new File("test/local/rawfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        out.write(new byte[] { (byte)-120, (byte)0, (byte)40});//0-3
        out.writeBoolean(false);//3-4
        out.writeBoolean(true);//4-5
        out.writeByte((byte)-60);//5-6
        out.writeByte((byte)30);//6-7
        out.writeShort((short)-4520);//7-9
        out.writeShort((short)31001);//9-11
        out.writeInt(0x2490F939);//11-15
        out.writeInt(0xB490F979);//15-19
        assert (0xB490F979 < 0); // if I'm not insane..
        out.writeLong(0xD02E908A2490F939L);//19-27
        assert (0xD02E908A2490F939L < 0);
        out.writeLong(0x102E908A2490F9EAL);//27-35
        
        out.writeFloat(0.00345f); // 35-39
        out.writeFloat(-98724957.34f); // 39-43

        out.writeDouble(0.00000000345d); // 43-51
        out.writeDouble(-9872495734907234324.09d); // 51-59

        out.flush();
        out.close();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        file.delete();
        file = null;
    }
    
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
