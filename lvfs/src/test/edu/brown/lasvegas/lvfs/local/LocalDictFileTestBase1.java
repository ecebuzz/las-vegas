package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.util.ChecksumUtil;

/**
 * Base class of testcases for {@link LocalDictFile}.
 * Because some type has limited value domains, some testcase has to be defined in derived classes.
 * See LocalDictFileTestBase2 and LocalDictFileTestBase4.
 */
public abstract class LocalDictFileTestBase1<T extends Comparable<T>, AT> {
    protected abstract T generateValue (int index, int dv);
    protected abstract ValueTraits<T, AT> getTraits ();
    protected abstract TypedWriter<T, AT> getWriter (VirtualFile file, int collectPerBytes) throws IOException;
    protected abstract TypedReader<T, AT> getReader (VirtualFile file) throws IOException;
    
    /** deterministic random values. */
    protected static int[] randoms = new int[1 << 18]; // don't use more than this
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Random r = new Random (12345L); // given seed for reproducibility
        for (int i = 0; i < randoms.length; ++i) {
            randoms[i] = Math.abs(r.nextInt());
        }
    }
    
    @Before
    public void setUp() throws Exception {
        dataFile = new LocalVirtualFile("test/local/test.data");
        compDataFile = new LocalVirtualFile("test/local/test.data.comp");
        dictFile = new LocalVirtualFile("test/local/test.dict");
        if (!dictFile.getParentFile().exists() && !dictFile.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + dictFile.getParentFile().getAbsolutePath());
        }
        dataFile.delete();
        compDataFile.delete();
        dictFile.delete();
        traits = getTraits();
    }
    protected VirtualFile dataFile;
    protected VirtualFile compDataFile;
    protected VirtualFile dictFile;
    protected ValueTraits<T, AT> traits;

    protected void createOriginalFile (int count, int dv) throws IOException {
        TypedWriter<T, AT> writer = getWriter (dataFile, 1024);
        writer.setCRC32Enabled(true);
        for (int i = 0; i < count; ++i) {
            writer.writeValue(generateValue(i, dv));
        }
        long crc32 = writer.writeFileFooter();
        assertTrue (crc32 != 0);
        writer.flush();
        writer.close();
        long correctCrc32 = ChecksumUtil.getFileCheckSum(dataFile);
        assertEquals (correctCrc32, crc32);
    }

    @Test
    public void testByteDict() throws Exception {
        final int COUNT = 3000;
        
        int dv = 256;
        createOriginalFile (COUNT, dv);
        TypedReader<T, AT> dataReader = getReader (dataFile);
        LocalDictFile<T, AT> dict = new LocalDictFile<T, AT> (dataReader, traits);
        dict.writeToFile(dictFile);
        assertEquals (1, dict.getBytesPerEntry());
        for (int key : new int[] {2431, 124, 879, 452, 2, 50, 0, 128}) {
            assert (key < COUNT);
            assertTrue(dict.compress(generateValue(key, dv)) !=  null);
        }

        {
            dataReader.seekToTupleAbsolute(0);
            AT srcBuf = traits.createArray(COUNT);
            int read = dataReader.readValues(srcBuf, 0, COUNT);
            assertEquals (COUNT, read);
            byte[] compressedBuf = new byte[COUNT];
            dict.compressBatch(srcBuf, 0, compressedBuf, 0, COUNT);
            LocalFixLenWriter<Byte, byte[]> dataWriter = LocalFixLenWriter.getInstanceTinyint(compDataFile);
            dataWriter.writeValues(compressedBuf, 0, COUNT);
            dataWriter.writeFileFooter();
            dataWriter.flush();
            dataWriter.close();
        }
        assertEquals (COUNT * 1, compDataFile.length());
        LocalFixLenReader<Byte, byte[]> compReader = LocalFixLenReader.getInstanceTinyint(compDataFile);
        dataReader.seekToTupleAbsolute(0);
        for (int i = 0; i < COUNT; ++i) {
            byte comp = compReader.readValue();
            T decomp = dict.decompress(comp);
            T original = dataReader.readValue();
            assertEquals (original, decomp);
            assertEquals (comp, dict.compress(decomp).intValue());
        }
    }
}
