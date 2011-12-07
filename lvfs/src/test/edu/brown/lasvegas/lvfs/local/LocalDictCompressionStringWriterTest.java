package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Random;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Testcases for {@link LocalDictCompressionStringWriter}.
 */
public class LocalDictCompressionStringWriterTest {

    private int distinctValues;
    private VirtualFile dataFile, dictFile;
    private void init(int distinctValues) throws Exception {
        this.distinctValues = distinctValues;

        dataFile = new LocalVirtualFile("test/local/str_dict.dat");
        if (!dataFile.getParentFile().exists() && !dataFile.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + dataFile.getParentFile().getAbsolutePath());
        }
        dataFile.delete();
        dictFile = new LocalVirtualFile("test/local/str_dict.dat.dict");
        dictFile.delete();
        File tmp = new File("test/local/str_dict.dat.tmp");
        tmp.delete();
        LocalDictCompressionStringWriter writer = new LocalDictCompressionStringWriter(dataFile, dictFile, tmp);
        
        Random rand = getDeterministicRandom();
        for (int i = 0; i < COUNT; ++i) {
            writer.writeValue(generateRandom(rand));
        }
        writer.writeFileFooter();
        writer.flush();
        writer.close();
    }
    private static Random getDeterministicRandom () {
        return new Random (22331L); // fixed seed;
    }
    private String getOrderedValue (int order) {
        assert (order >= 0);
        assert (order < distinctValues);
        return "key-" + String.format("%06d", order);
    }
    
    private String generateRandom (Random rand) {
        int r = rand.nextInt();
        if (r < 0) r = -r;
        return getOrderedValue(r % distinctValues);
    }
    private final static int COUNT = 75000;
    @Test
    public void testByteDict() throws Exception {
        init (256);
        assertEquals (1 * COUNT, dataFile.length());
        LocalFixLenReader<Byte, byte[]> reader = LocalFixLenReader.getInstanceTinyint(dataFile);
        LocalDictFile dict = new LocalDictFile(dictFile);
        assertEquals (1, dict.getBytesPerEntry());
        assertTrue (dict.getDictionary().length <= 256);
        assertTrue (dict.getDictionary().length > 128);
        Random rand = getDeterministicRandom();
        byte[] buf = new byte[1 << 14];
        int i = 0;
        while (true) {
            int read = reader.readValues(buf, 0, buf.length);
            if (read <= 0) {
                break;
            }
            for (int j = 0; j < read; ++j) {
                String value = dict.decompress(buf[j]);
                String correct = generateRandom(rand);
                assertEquals (correct, value);
            }
            i += read;
        }
        assertEquals (COUNT, i);
        reader.close();
    }

    @Test
    public void testShortDict() throws Exception {
        init (1 << 16);
        assertEquals (2 * COUNT, dataFile.length());
        LocalFixLenReader<Short, short[]> reader = LocalFixLenReader.getInstanceSmallint(dataFile);
        LocalDictFile dict = new LocalDictFile(dictFile);
        assertEquals (2, dict.getBytesPerEntry());
        assertTrue (dict.getDictionary().length <= (1 << 16));
        assertTrue (dict.getDictionary().length > 256);
        Random rand = getDeterministicRandom();
        short[] buf = new short[1 << 14];
        int i = 0;
        while (true) {
            int read = reader.readValues(buf, 0, buf.length);
            if (read <= 0) {
                break;
            }
            for (int j = 0; j < read; ++j) {
                String value = dict.decompress(buf[j]);
                String correct = generateRandom(rand);
                assertEquals (correct, value);
            }
            i += read;
        }
        assertEquals (COUNT, i);
        reader.close();
    }

    @Test
    public void testIntDict() throws Exception {
        init (0x7FFFFFFF);
        assertEquals (4 * COUNT, dataFile.length());
        LocalFixLenReader<Integer, int[]> reader = LocalFixLenReader.getInstanceInteger(dataFile);
        LocalDictFile dict = new LocalDictFile(dictFile);
        assertEquals (4, dict.getBytesPerEntry());
        assertTrue (dict.getDictionary().length <= (0x7FFFFFF));
        assertTrue (dict.getDictionary().length > (1 << 16));
        Random rand = getDeterministicRandom();
        int[] buf = new int[1 << 14];
        int i = 0;
        while (true) {
            int read = reader.readValues(buf, 0, buf.length);
            if (read <= 0) {
                break;
            }
            for (int j = 0; j < read; ++j) {
                String value = dict.decompress(buf[j]);
                String correct = generateRandom(rand);
                assertEquals (correct, value);
            }
            i += read;
        }
        assertEquals (COUNT, i);
        reader.close();
    }
}
