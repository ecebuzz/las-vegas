package edu.brown.lasvegas.lvfs.data;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;

/**
 * Testcases for {@link PartitionedTextFileWriters} (actually and {@link PartitionedTextFileReaders}).
 */
public class PartitionedTextFileWritersTest {
    private static final File outputDir = new File("test/part");
    private static final int nodeId = 22;
    private static final int groupId = 44;
    private static final int fractureId = 66;
    private static final int writeBufferSize = 256;
    private static final int writePartitionsMax = 10;
    private static final int LINES = 10000;

    @Before
    public void setUp() throws Exception {
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        } else {
            for (File child : outputDir.listFiles()) {
                child.delete();
            }
        }
    }
    
    @Test
    public void testWriters () throws IOException {
        testWritersInternal (3, CompressionType.NONE);
    }
    @Test
    public void testWritersMulti () throws IOException {
        testWritersInternal (15, CompressionType.NONE);
    }
    @Test
    public void testWritersSnappy () throws IOException {
        testWritersInternal (3, CompressionType.SNAPPY);
    }
    @Test
    public void testWritersSnappyMulti () throws IOException {
        testWritersInternal (15, CompressionType.SNAPPY);
    }
    @Test
    public void testWritersGzip () throws IOException {
        testWritersInternal (3, CompressionType.GZIP_BEST_COMPRESSION);
    }
    @Test
    public void testWritersGzipMulti () throws IOException {
        testWritersInternal (15, CompressionType.GZIP_BEST_COMPRESSION);
    }
    private void testWritersInternal (int partitionCount, CompressionType compression) throws IOException {
        boolean[] partitionCompleted = new boolean[partitionCount];
        Arrays.fill(partitionCompleted, false);
        PartitionedTextFileWriters writers = new PartitionedTextFileWriters(outputDir, nodeId, groupId, fractureId, partitionCount, partitionCompleted, "UTF-8", writeBufferSize, writePartitionsMax, compression);
        for (int i = 0; i < LINES; ++i) {
            writers.write(i % partitionCount, "lineline" + i);
        }
        String[] files = writers.complete();
        assertEquals (partitionCount <= writePartitionsMax ? partitionCount : writePartitionsMax, files.length);
        for (int i = 0; i < files.length; ++i) {
            validateWrittenFile(files[i], partitionCount, i, compression);
        }
        if (partitionCount <= writePartitionsMax) {
            assertFalse(writers.isPartitionRemaining());
            return;
        }
        assertTrue(writers.isPartitionRemaining());
        // do it once again for another partitions set of partitions
        Arrays.fill(partitionCompleted, 0, writePartitionsMax, true);
        writers = new PartitionedTextFileWriters(outputDir, nodeId, groupId, fractureId, partitionCount, partitionCompleted, "UTF-8", writeBufferSize, writePartitionsMax, compression);
        for (int i = 0; i < LINES; ++i) {
            writers.write(i % partitionCount, "lineline" + i);
        }
        files = writers.complete();
        assertEquals (partitionCount - writePartitionsMax <= writePartitionsMax ? partitionCount - writePartitionsMax : writePartitionsMax, files.length);
        for (int i = 0; i < files.length; ++i) {
            validateWrittenFile(files[i], partitionCount, i + writePartitionsMax, compression);
        }
        if (partitionCount - writePartitionsMax <= writePartitionsMax) {
            assertFalse(writers.isPartitionRemaining());
        } else {
            assertTrue(writers.isPartitionRemaining());
        }
    }
    private void validateWrittenFile (String file, int partitionCount, int partititon, CompressionType compression) throws IOException {
        TemporaryFilePath tmpPath = new TemporaryFilePath(file);
        assertEquals (nodeId, tmpPath.nodeId);
        assertEquals (groupId, tmpPath.replicaGroupId);
        assertEquals (fractureId, tmpPath.fractureId);
        assertEquals (compression, tmpPath.compression);
        assertEquals (outputDir.getAbsolutePath(), tmpPath.folderPath);
        assertEquals (partititon, tmpPath.partition);
        PartitionedTextFileReader reader = new PartitionedTextFileReader(new LocalVirtualFile(file), Charset.forName("UTF-8"), compression, 1 << 16);
        for (int i = partititon; i < LINES; i += partitionCount) {
            String line = reader.readLine();
            assertEquals ("lineline" + i, line);
        }
        assertNull (reader.readLine());
        reader.close();
    }
}
