package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.TypedReader;

/**
 * For types with at least 4 bytes. 4 bytes has enough value domains.
 */
public abstract class LocalDictFileTestBase4<T extends Comparable<T>, AT> extends LocalDictFileTestBase2<T, AT> {
    @Test
    public void testIntDict() throws Exception {
        final int COUNT = 85000; // should be large enough to have >65536 distinct values
        
        int dv = Integer.MAX_VALUE;
        createOriginalFile (COUNT, dv);
        TypedReader<T, AT> dataReader = getReader (dataFile);
        LocalDictFile<T, AT> dict = new LocalDictFile<T, AT> (dataReader, traits);
        dict.writeToFile(dictFile);
        assertEquals (4, dict.getBytesPerEntry());
        for (int key : new int[] {54314, 23423, 665, 84231, 452, 7841, 1200, 0, 128}) {
           assert (key < COUNT);
            assertTrue(dict.compress(generateValue(key, dv)) !=  null);
        }

        {
            dataReader.seekToTupleAbsolute(0);
            AT srcBuf = traits.createArray(COUNT);
            int read = dataReader.readValues(srcBuf, 0, COUNT);
            assertEquals (COUNT, read);
            int[] compressedBuf = new int[COUNT];
            dict.compressBatch(srcBuf, 0, compressedBuf, 0, COUNT);
            LocalFixLenWriter<Integer, int[]> dataWriter = LocalFixLenWriter.getInstanceInteger(compDataFile);
            dataWriter.writeValues(compressedBuf, 0, COUNT);
            dataWriter.writeFileFooter();
            dataWriter.flush();
            dataWriter.close();
        }
        assertEquals (COUNT * 4, compDataFile.length());
        LocalFixLenReader<Integer, int[]> compReader = LocalFixLenReader.getInstanceInteger(compDataFile);
        dataReader.seekToTupleAbsolute(0);
        for (int i = 0; i < COUNT; ++i) {
            int comp = compReader.readValue();
            T decomp = dict.decompress(comp);
            T original = dataReader.readValue();
            assertEquals (original, decomp);
            assertEquals (comp, dict.compress(decomp).intValue());
        }
    }
}
