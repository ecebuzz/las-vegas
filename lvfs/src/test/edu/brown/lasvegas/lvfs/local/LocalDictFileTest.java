package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * Testcase for {@link LocalDictFile}.
 */
public class LocalDictFileTest {
    private static String generateValue (int index, int dv) {
        int rand = randoms[index] % dv;
        if (rand < 0) rand = -rand;
        // to keep the <, > relationship, pad with zeros
        String paddedRand = String.format("%08d", rand);
        assert (paddedRand.indexOf('-') < 0);
        return ("str" + paddedRand + "ad");
    }
    
    /** deterministic random values. */
    private static int[] randoms = new int[1 << 18]; // don't use more than this
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Random r = new Random (12345L); // given seed for reproducibility
        for (int i = 0; i < randoms.length; ++i) {
            randoms[i] = r.nextInt();
        }
    }
    
    @Before
    public void setUp() throws Exception {
        dataFile = new File("test/local/test.data");
        compDataFile = new File("test/local/test.data.comp");
        dictFile = new File("test/local/test.dict");
        if (!dictFile.getParentFile().exists() && !dictFile.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + dictFile.getParentFile().getAbsolutePath());
        }
        dataFile.delete();
        compDataFile.delete();
        dictFile.delete();
    }
    private File dataFile;
    private File compDataFile;
    private File dictFile;

    private void createOriginalFile (int count, int dv) throws IOException {
        LocalVarLenWriter<String> writer = LocalVarLenWriter.getInstanceVarchar(dataFile, 1024);
        for (int i = 0; i < count; ++i) {
            writer.writeValue(generateValue(i, dv));
        }
        writer.writeFileFooter();
        writer.flush();
        writer.close();
    }

    @Test
    public void testByteDict() throws Exception {
        final int COUNT = 3000;
        
        int dv = 256;
        createOriginalFile (COUNT, dv);

        LocalVarLenReader<String> dataReader = LocalVarLenReader.getInstanceVarchar(dataFile);
        LocalDictFile.createVarcharDictFile(dictFile, dataReader);
        LocalDictFile dict = new LocalDictFile(dictFile);
        assertEquals (1, dict.getBytesPerEntry());
        for (int key : new int[] {2431, 124, 879, 452, 2, 50, 0, 128}) {
            assert (key < COUNT);
            assertTrue(dict.compress(generateValue(key, dv)) !=  null);
        }

        {
            dataReader.seekToTupleAbsolute(0);
            LocalFixLenWriter<Byte, byte[]> dataWriter = LocalFixLenWriter.getInstanceTinyint(compDataFile);
            dict.compressVarcharFileByte(dataReader, dataWriter);
            dataWriter.writeFileFooter();
            dataWriter.flush();
            dataWriter.close();
        }
        assertEquals (COUNT * 1, compDataFile.length());
        LocalFixLenReader<Byte, byte[]> compReader = LocalFixLenReader.getInstanceTinyint(compDataFile);
        dataReader.seekToTupleAbsolute(0);
        for (int i = 0; i < COUNT; ++i) {
            byte comp = compReader.readValue();
            String decomp = dict.decompress(comp);
            String original = dataReader.readValue();
            assertEquals (original, decomp);
            assertEquals (comp, dict.compress(decomp).intValue());
        }
    }
    
    @Test
    public void testShortDict() throws Exception {
        final int COUNT = 25000;
        
        int dv = 1 << 16;
        createOriginalFile (COUNT, dv);

        LocalVarLenReader<String> dataReader = LocalVarLenReader.getInstanceVarchar(dataFile);
        LocalDictFile.createVarcharDictFile(dictFile, dataReader);
        LocalDictFile dict = new LocalDictFile(dictFile);
        assertEquals (2, dict.getBytesPerEntry());
        for (int key : new int[] {5431, 23423, 665, 23423, 452, 7841, 1200, 0, 128}) {
            assert (key < COUNT);
            assertTrue(dict.compress(generateValue(key, dv)) !=  null);
        }

        {
            dataReader.seekToTupleAbsolute(0);
            LocalFixLenWriter<Short, short[]> dataWriter = LocalFixLenWriter.getInstanceSmallint(compDataFile);
            dict.compressVarcharFileShort(dataReader, dataWriter);
            dataWriter.writeFileFooter();
            dataWriter.flush();
            dataWriter.close();
        }
        assertEquals (COUNT * 2, compDataFile.length());
        LocalFixLenReader<Short, short[]> compReader = LocalFixLenReader.getInstanceSmallint(compDataFile);
        dataReader.seekToTupleAbsolute(0);
        for (int i = 0; i < COUNT; ++i) {
            short comp = compReader.readValue();
            String decomp = dict.decompress(comp);
            String original = dataReader.readValue();
            assertEquals (original, decomp);
            assertEquals (comp, dict.compress(decomp).intValue());
        }
    }

    @Test
    public void testIntDict() throws Exception {
        final int COUNT = 85000; // should be large enough to have >65536 distinct values
        
        int dv = Integer.MAX_VALUE;
        createOriginalFile (COUNT, dv);

        LocalVarLenReader<String> dataReader = LocalVarLenReader.getInstanceVarchar(dataFile);
        LocalDictFile.createVarcharDictFile(dictFile, dataReader);
        LocalDictFile dict = new LocalDictFile(dictFile);
        assertEquals (4, dict.getBytesPerEntry());
        for (int key : new int[] {54314, 23423, 665, 84231, 452, 7841, 1200, 0, 128}) {
            assert (key < COUNT);
            assertTrue(dict.compress(generateValue(key, dv)) !=  null);
        }

        {
            dataReader.seekToTupleAbsolute(0);
            LocalFixLenWriter<Integer, int[]> dataWriter = LocalFixLenWriter.getInstanceInteger(compDataFile);
            dict.compressVarcharFileInt(dataReader, dataWriter);
            dataWriter.writeFileFooter();
            dataWriter.flush();
            dataWriter.close();
        }
        assertEquals (COUNT * 4, compDataFile.length());
        LocalFixLenReader<Integer, int[]> compReader = LocalFixLenReader.getInstanceInteger(compDataFile);
        dataReader.seekToTupleAbsolute(0);
        for (int i = 0; i < COUNT; ++i) {
            int comp = compReader.readValue();
            String decomp = dict.decompress(comp);
            String original = dataReader.readValue();
            assertEquals (original, decomp);
            assertEquals (comp, dict.compress(decomp).intValue());
        }
    }
}
