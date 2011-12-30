package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

/**
 * For types with at least 2 bytes.
 */
public abstract class LocalDictCompressionWriterTestBase2<T extends Comparable<T>, AT> extends LocalDictCompressionWriterTestBase1<T, AT> {
    @Test
    public void testShortDict() throws Exception {
        init (1 << 16);
        assertEquals (2 * COUNT, dataFile.length());
        LocalFixLenReader<Short, short[]> reader = LocalFixLenReader.getInstanceSmallint(dataFile);
        LocalDictFile<T, AT> dict = new LocalDictFile<T, AT>(dictFile, traits);
        assertEquals (2, dict.getBytesPerEntry());
        assertTrue (dict.getDictionarySize() <= (1 << 16));
        assertTrue (dict.getDictionarySize() > 256);
        Random rand = getDeterministicRandom();
        short[] buf = new short[1 << 14];
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
