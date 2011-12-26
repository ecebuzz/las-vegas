package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.lvfs.local.RawByteArrayOutputStream;

/**
 * A set of writers to write out temporary partitioned text files.
 * In order to limit memory usage, this class writes out only a given
 * number of partitions at once. If num of partitions is larger than
 * the number (writePartitionsMax), the user should scan the input
 * files and call this class more than once.
 */
public final class PartitionedTextFileWriters {
    private static Logger LOG = Logger.getLogger(PartitionedTextFileWriters.class);
    
    public PartitionedTextFileWriters (File outputDir,
                    int nodeId, LVReplicaGroup group, LVFracture fracture,
                    int partitions, boolean[] partitionCompleted, String encoding,
                    int writeBufferSize, int writePartitionsMax, CompressionType compression) {
        this.outputDir = outputDir;
        this.nodeId = nodeId;
        this.group = group;
        this.fracture = fracture;
        writers = new PartitionWriter[partitions];
        this.partitionCompleted = partitionCompleted.clone();
        assert (partitions == partitionCompleted.length);
        charset = Charset.forName(encoding);
        this.writeBufferSize = writeBufferSize;
        this.writePartitionsMax = writePartitionsMax;
        this.compression = compression;
        this.compressionBuffer = new byte[writeBufferSize * 12 / 10];
    }
    /** parent folder to place the output file. */
    private final File outputDir;

    /** only used to determine the output file name. */
    private final int nodeId;
    /** only used to determine the output file name. */
    private final LVReplicaGroup group;
    /** only used to determined the output file name. */
    private final LVFracture fracture;
    /** index=partition (from zero). */
    private final PartitionWriter[] writers;
    /** count of partitions being written out now. */
    private int writerCount = 0;
    /** is the partition already written out in the previous iteration? */
    private final boolean[] partitionCompleted;
    /** is some partition not written out yet? */
    private boolean partitionRemaining = false;
    

    private final Charset charset;
    private final int writeBufferSize;
    private final int writePartitionsMax;
    private final CompressionType compression;
    /** temporary buffer to write out a compressed block. */
    private byte[] compressionBuffer;

    /** Write one tuple to the corresponding partition file. */
    public void write (int partition, String line) throws IOException {
        if (partitionCompleted[partition]) {
            return;
        }
        if (writers[partition] == null) {
            if (writerCount < writePartitionsMax) {
                writers[partition] = new PartitionWriter(partition);
                ++writerCount;
            } else {
                partitionRemaining = true;
                return;
            }
        }
        writers[partition].write (line);
    }
    
    /**
     * Close all writers and report the written file paths.
     * @return full path of written files.
     */
    public String[] complete () throws IOException {
        List<String> paths = new ArrayList<String>();
        for (int i = 0; i < writers.length; ++i) {
            PartitionWriter writer = writers[i];
            if (writer == null) {
                continue;
            }
            writer.close();
            partitionCompleted[i] = true;
            paths.add(writer.file.getAbsolutePath());
        }
        return paths.toArray(new String[0]);
    }

    /** a writer for each partition. */
    public class PartitionWriter {
        public PartitionWriter(int partition) throws IOException {
            buffer = new byte[writeBufferSize];
            TemporaryFilePath fileName = new TemporaryFilePath(outputDir.getAbsolutePath(), nodeId, group.getGroupId(), fracture.getFractureId(), partition, new Random (System.nanoTime()).nextInt(), compression);
            file = new File(fileName.getFilePath());
            out = new FileOutputStream(file, false);
        }
        private final File file;
        private final FileOutputStream out;
        private final byte[] buffer;
        private int bufferUsed = 0;
        public void write (String line) throws IOException {
            byte[] bytes = (line + "\r\n").getBytes(charset);
            if (bufferUsed + bytes.length > buffer.length) {
                flushBlock ();
            }
            System.arraycopy(bytes, 0, buffer, bufferUsed, bytes.length);
        }
        private void flushBlock () throws IOException {
            // flush current buffer as a new block (if compressed, also put size before/after compression as a header)
            if (compression == CompressionType.SNAPPY) {
                int sizeAfterCompression = Snappy.compress(buffer, 0, bufferUsed, compressionBuffer, 8);
                if (sizeAfterCompression > compressionBuffer.length) {
                    // this might happen, but not sure how Snappy-java handles exceptional cases..
                    throw new IOException ("compression buffer too small???");
                }
                LOG.info("compressed a block in snappy:" + bufferUsed + " -> " + sizeAfterCompression);
                ByteBuffer.wrap(compressionBuffer).asIntBuffer().put(new int[]{bufferUsed, sizeAfterCompression});
                out.write(compressionBuffer, 0, 8 + sizeAfterCompression);
            } else if (compression == CompressionType.GZIP_BEST_COMPRESSION) {
                RawByteArrayOutputStream rawBuffer = new RawByteArrayOutputStream(compressionBuffer, 8);
                // directly specify level by double brace initialization (kind of a hack)
                GZIPOutputStream gzip = new GZIPOutputStream(rawBuffer) {{
                    def.setLevel(Deflater.BEST_COMPRESSION);
                }};
                gzip.write(buffer, 0, bufferUsed);
                gzip.flush();
                gzip.close();
                compressionBuffer = rawBuffer.getRawBuffer(); // in case ByteArrayOutputStream expanded it
                int sizeAfterCompression = rawBuffer.size() - 8;
                LOG.info("compressed a block in gzip:" + bufferUsed + " -> " + sizeAfterCompression);
                ByteBuffer.wrap(compressionBuffer).asIntBuffer().put(new int[]{bufferUsed, sizeAfterCompression});
                out.write(compressionBuffer, 0, 8 + sizeAfterCompression);
            } else {
                out.write(buffer, 0, bufferUsed);
            }
            bufferUsed = 0;
        }
        public void close () throws IOException {
            flushBlock();
            out.flush();
            out.close();
        }
    }

 // auto-generated getters/setters (comments by JAutodoc)
    
    /**
     * Gets the is the partition already written out in the previous iteration?.
     *
     * @return the is the partition already written out in the previous iteration?
     */
    public boolean[] getPartitionCompleted() {
        return partitionCompleted;
    }

    /**
     * Checks if is is some partition not written out yet?.
     *
     * @return the is some partition not written out yet?
     */
    public boolean isPartitionRemaining() {
        return partitionRemaining;
    }
}
