package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Random;

import org.junit.Test;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;

import edu.brown.lasvegas.protocol.LVDataProtocol;

/**
 * Base class of testcases for {@link DataEngine}.
 * Name of this abstract class doesn't end with Test so that our ant script
 * would skip this. 
 */
public abstract class DataEngineTestBase {
    private static String tmpDir;
    protected static LVDataProtocol dataProtocol;
    
    private static final String FILE1_NAME = "file1";
    private static final int FILE1_SIZE = 321;
    private static final int FILE1_SEED = 34501;
    
    private static final String FILE2_DIR = "dir2";
    private static final String FILE2_NAME = "dir2/file2";
    private static final int FILE2_SIZE = 1000;
    private static final int FILE2_SEED = 323;

    protected static void setDataNodeDirs (String root, String tmp) {
        tmpDir = tmp;
    }
    protected static void createRandomFiles () throws Exception {
        writeRandomFile (new File (tmpDir, FILE1_NAME), FILE1_SIZE, FILE1_SEED);
        writeRandomFile (new File (tmpDir, FILE2_NAME), FILE2_SIZE, FILE2_SEED);
    }
    private static void writeRandomFile (File file, int fileSize, int fileSeed) throws IOException {
        byte[] bytes = new byte[fileSize * 4];
        IntBuffer data = ByteBuffer.wrap(bytes).asIntBuffer();
        Random rand = new Random(fileSeed);
        for (int i = 0; i < fileSize; ++i) {
            data.put(rand.nextInt());
        }
        file.getParentFile().mkdirs();
        assertTrue(file.getParentFile().exists());
        FileOutputStream out = new FileOutputStream(file, false);
        out.write(bytes);
        out.flush();
        out.close();
        assert (file.length() == fileSize * 4);
    }
/*
    @Test
    public void testGetProtocolSignature() throws IOException {
        ProtocolSignature signature = dataProtocol.getProtocolSignature(LVDataProtocol.class.getName(), LVDataProtocol.versionID, 1234);
        assertEquals(LVDataProtocol.versionID, signature.getVersion());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetProtocolVersion() throws IOException {
        long version = dataProtocol.getProtocolVersion(LVDataProtocol.class.getName(), LVDataProtocol.versionID);
        assertEquals(LVDataProtocol.versionID, version);
    }
*/
    private static final int VALIDATE_BUF_SIZE = 30;
    private void validateRandomFile (String path, int fileSize, int fileSeed) throws IOException {
        assertTrue(dataProtocol.existsFile(path));
        assertEquals (fileSize * 4, dataProtocol.getFileLength(path));
        
        byte[] answer = new byte[fileSize * 4];
        IntBuffer data = ByteBuffer.wrap(answer).asIntBuffer();
        Random rand = new Random(fileSeed);
        for (int i = 0; i < fileSize; ++i) {
            data.put(rand.nextInt());
        }

        for (int cur = 0; cur != fileSize * 4;) {
            byte[] bytes = dataProtocol.getFileBody(path, cur, VALIDATE_BUF_SIZE * 4);
            assertTrue ("cur=" + cur + ", bytes.length=" + bytes.length, cur + bytes.length == fileSize * 4|| bytes.length == VALIDATE_BUF_SIZE * 4);
            for (int i = 0; i < bytes.length; ++i) {
                assertEquals ("cur=" + cur + ", i=" + i, answer[i + cur], bytes[i]);
            }
            cur += bytes.length;
        }
    }
    @Test
    public void testGetFileBody() throws IOException {
        validateRandomFile (tmpDir + "/" + FILE1_NAME, FILE1_SIZE, FILE1_SEED);
        validateRandomFile (tmpDir + "/" + FILE2_NAME, FILE2_SIZE, FILE2_SEED);
        try {
            dataProtocol.getFileBody(tmpDir + "/" + "dummy", 0, 100);
            fail();
        } catch (IOException ex) {
            // assertTrue (ex instanceof FileNotFoundException); the exception re-thrown by RPC loses the type information
        }
    }

    @Test
    public void testGetFileInputStream () throws Exception {
        testGetFileInputStreamInternal (tmpDir + "/" + FILE1_NAME, FILE1_SIZE, FILE1_SEED);
        testGetFileInputStreamInternal (tmpDir + "/" + FILE2_NAME, FILE2_SIZE, FILE2_SEED);
        try {
            testGetFileInputStreamInternal (tmpDir + "/" + "dummy", 0, 100);
            fail();
        } catch (Exception ex) {
        }
    }

    private void testGetFileInputStreamInternal (String path, int fileSize, int fileSeed) throws Exception {
        File destFile = new File (tmpDir, "dest");
        destFile.delete();
        RemoteInputStream inFile = dataProtocol.getFileInputStream(path);
        InputStream wrapped = RemoteInputStreamClient.wrap(inFile);
        try {
            FileOutputStream out = new FileOutputStream(destFile);
            byte[] bytes = new byte[1 << 13];
            while (true) {
                int read = wrapped.read(bytes);
                if (read < 0) {
                    break;
                }
                out.write(bytes, 0, read);
            }
            out.flush();
            out.close();
        } finally {
            wrapped.close();
        }
        validateDestFile(destFile, fileSize, fileSeed);
    }
    private void validateDestFile (File destFile, int fileSize, int fileSeed) throws IOException {
        assertTrue(destFile.exists());
        assertEquals (fileSize * 4, destFile.length());
        
        byte[] answer = new byte[fileSize * 4];
        IntBuffer data = ByteBuffer.wrap(answer).asIntBuffer();
        Random rand = new Random(fileSeed);
        for (int i = 0; i < fileSize; ++i) {
            data.put(rand.nextInt());
        }

        FileInputStream in = new FileInputStream(destFile);
        byte[] destData = new byte[fileSize * 4];
        int read = in.read(destData);
        assertEquals (fileSize * 4, read);
        in.close();
        assertArrayEquals (answer, destData);
    }


    @Test
    public void testGetFileLength() throws IOException {
        assertEquals(FILE1_SIZE * 4, dataProtocol.getFileLength(tmpDir + "/" + FILE1_NAME));
        assertEquals(FILE2_SIZE * 4, dataProtocol.getFileLength(tmpDir + "/" + FILE2_NAME));
        assertEquals(0, dataProtocol.getFileLength (tmpDir + "/" + "dummy"));
    }

    @Test
    public void testExistsFile() throws IOException {
        assertTrue(dataProtocol.existsFile(tmpDir + "/" + FILE1_NAME));
        assertTrue(dataProtocol.existsFile(tmpDir + "/" + FILE2_NAME));
        assertFalse(dataProtocol.existsFile(tmpDir + "/" + "dummy"));
    }

    @Test
    public void testIsDirectory() throws IOException {
        assertFalse(dataProtocol.isDirectory(tmpDir + "/" + FILE1_NAME));
        assertFalse(dataProtocol.isDirectory(tmpDir + "/" + FILE2_NAME));
        assertFalse(dataProtocol.isDirectory(tmpDir + "/" + "dummy"));
        assertTrue(dataProtocol.isDirectory(tmpDir + "/" + FILE2_DIR));
    }

    @Test
    public void testGetCombinedFileStatus() throws IOException {
        assertArrayEquals(new int[]{FILE1_SIZE * 4, 1, 0}, dataProtocol.getCombinedFileStatus(tmpDir + "/" + FILE1_NAME));
        assertArrayEquals(new int[]{FILE2_SIZE * 4, 1, 0}, dataProtocol.getCombinedFileStatus(tmpDir + "/" + FILE2_NAME));
        assertArrayEquals(new int[]{0, 0, 0}, dataProtocol.getCombinedFileStatus(tmpDir + "/" + "dummy"));
        assertArrayEquals(new int[]{0, 1, 1}, dataProtocol.getCombinedFileStatus(tmpDir + "/" + FILE2_DIR));
    }

    @Test
    public void testDeleteFile() throws IOException {
        writeRandomFile (new File (tmpDir, "dir3/dir4/file5"), 100, 100);
        writeRandomFile (new File (tmpDir, "dir3/file6"), 100, 100);
        writeRandomFile (new File (tmpDir, "dir7/file8"), 100, 100);
        assertTrue (dataProtocol.existsFile(tmpDir + "/dir3"));
        assertTrue (dataProtocol.existsFile(tmpDir + "/dir7"));

        assertFalse (dataProtocol.deleteFile(tmpDir + "/dir3", false));
        assertTrue (dataProtocol.deleteFile(tmpDir + "/dir3", true));
        assertTrue (dataProtocol.deleteFile(tmpDir + "/dir7/file8", false));
        assertTrue (dataProtocol.deleteFile(tmpDir + "/dir7", false));

        assertFalse (dataProtocol.existsFile(tmpDir + "/dir3"));
        assertFalse (dataProtocol.existsFile(tmpDir + "/dir7"));
    }

}
