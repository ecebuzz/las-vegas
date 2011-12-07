package edu.brown.lasvegas.lvfs.local;

import java.io.DataOutputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Testcase for {@link LocalRawFileReader}.
 */
public class LocalRawFileReaderTest extends LocalRawFileTestBase {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // create the file to test
        file = new LocalVirtualFile("test/local/rawfile.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        DataOutputStream out = new DataOutputStream(file.getOutputStream());

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
}
