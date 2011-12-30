package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

/**
 * For types with at least 4 bytes.
 */
public abstract class LocalDictCompressionWriterTestBase4<T extends Comparable<T>, AT> extends LocalDictCompressionWriterTestBase2<T, AT> {
    @Test
    public void testIntDict() throws Exception {
        init (0x7FFFFFFF);
        assertEquals (4 * COUNT, dataFile.length());
        LocalFixLenReader<Integer, int[]> reader = LocalFixLenReader.getInstanceInteger(dataFile);
        LocalDictFile<T, AT> dict = new LocalDictFile<T, AT>(dictFile, traits);
        assertEquals (4, dict.getBytesPerEntry());
        assertTrue (dict.getDictionarySize() <= (0x7FFFFFF));
        assertTrue (dict.getDictionarySize() > (1 << 16));
        Random rand = getDeterministicRandom();
        int[] buf = new int[1 << 14];
        int i = 0;
        while (true) {
            int read = reader.readValues(buf, 0, buf.length);
            if (read <= 0) {
                break;
            }
            for (int j = 0; j < read; ++j) {
                T value = dict.decompress(buf[j]);
                T correct = generateRandom(rand);
                assertEquals (correct, value);
            }
            i += read;
        }
        assertEquals (COUNT, i);
        reader.close();
    }

}
