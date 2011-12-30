package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.TypedReader;

/**
 * For types with at least 2 bytes.
 */
public abstract class LocalDictFileTestBase2<T extends Comparable<T>, AT> extends LocalDictFileTestBase1<T, AT> {
    
    @Test
    public void testShortDict() throws Exception {
        final int COUNT = 25000;
        
        int dv = 1 << 16;
        createOriginalFile (COUNT, dv);
        TypedReader<T, AT> dataReader = getReader (dataFile);
        LocalDictFile<T, AT> dict = new LocalDictFile<T, AT> (dataReader, traits);
        dict.writeToFile(dictFile);
        assertEquals (2, dict.getBytesPerEntry());
        for (int key : new int[] {5431, 23423, 665, 23423, 452, 7841, 1200, 0, 128}) {
            assert (key < COUNT);
            assertTrue(dict.compress(generateValue(key, dv)) !=  null);
        }

        {
            dataReader.seekToTupleAbsolute(0);
            AT srcBuf = traits.createArray(COUNT);
            int read = dataReader.readValues(srcBuf, 0, COUNT);
            assertEquals (COUNT, read);
            short[] compressedBuf = new short[COUNT];
            dict.compressBatch(srcBuf, 0, compressedBuf, 0, COUNT);
            LocalFixLenWriter<Short, short[]> dataWriter = LocalFixLenWriter.getInstanceSmallint(compDataFile);
            dataWriter.writeValues(compressedBuf, 0, COUNT);
            dataWriter.writeFileFooter();
            dataWriter.flush();
            dataWriter.close();
        }
        assertEquals (COUNT * 2, compDataFile.length());
        LocalFixLenReader<Short, short[]> compReader = LocalFixLenReader.getInstanceSmallint(compDataFile);
        dataReader.seekToTupleAbsolute(0);
        for (int i = 0; i < COUNT; ++i) {
            short comp = compReader.readValue();
            T decomp = dict.decompress(comp);
            T original = dataReader.readValue();
            assertEquals (original, decomp);
            assertEquals (comp, dict.compress(decomp).intValue());
        }
    }

}
