package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.util.RawByteArrayOutputStream;

/**
 * Writes out temporary partitioned text files.
 */
public final class PartitionedTextFileWriters implements Closeable {
    private static Logger LOG = Logger.getLogger(PartitionedTextFileWriters.class);
    
    public PartitionedTextFileWriters (File outputDir, int maxFragments,
                    int nodeId, int groupId, int fractureId,
                    int partitions, String encoding,
                    long writeBufferSizeTotal, CompressionType compression) {
        this.outputDir = outputDir;
        this.nodeId = nodeId;
        this.groupId = groupId;
        this.fractureId = fractureId;
        this.partitions = partitions;
        this.charset = Charset.forName(encoding);
        this.recusriveTmpFolder = new File(outputDir, "recursive_tmp_" + Math.abs( new Random (System.nanoTime()).nextInt()));
        this.partitioner = new RecursiveTextFilePartitioner(recusriveTmpFolder, maxFragments, 0, partitions, charset, writeBufferSizeTotal);
        
        this.compression = compression;
    }
    /** parent folder to place the output file. */
    private final File outputDir;
    private final File recusriveTmpFolder;

    /** only used to determine the output file name. */
    private final int nodeId;
    /** only used to determine the output file name. */
    private final int groupId;
    /** only used to determined the output file name. */
    private final int fractureId;

    /** the object that implements most of recursive partitioning. */
    private final RecursiveTextFilePartitioner partitioner;    

    private final int partitions;
    private final Charset charset;
    private final CompressionType compression;

    /** Write one tuple to the corresponding partition file. */
    public void write (int partition, String line) throws IOException {
        partitioner.write(partition, line);
    }
    
    /**
     * Close all writers and report the written file paths.
     * @return full path of written files.
     */
    public String[] complete () throws IOException {
        String[] tmpPaths = partitioner.complete();
        assert (tmpPaths.length == partitions);
        String[] paths = new String[partitions];

        byte[] buffer = null;
        byte[] compressionBuffer = null;
        if (compression != CompressionType.NONE) {
            buffer = new byte[1 << 20];
            compressionBuffer = new byte[buffer.length * 12 / 10];
        }

        for (int partition = 0; partition < partitions; ++partition) {
            String tmpPath = tmpPaths[partition];
            if (tmpPath == null) {
                continue;
            }
            TemporaryFilePath fileName = new TemporaryFilePath(outputDir.getAbsolutePath(),
                nodeId, groupId, fractureId, partition, Math.abs( new Random (System.nanoTime()).nextInt()), compression);
            File file = new File(fileName.getFilePath());
            File tmpFile = new File(tmpPath);
            assert(new File(tmpPath).exists());
            if (compression == CompressionType.NONE) {
                // then it's just a rename
                boolean renamed = tmpFile.renameTo(file);
                assert (renamed);
            } else {
                // otherwise, we need to compress the file.
                compressTmpFile(file, tmpFile, buffer, compressionBuffer);
                tmpFile.delete(); //no longer needed
            }
            paths[partition] = file.getAbsolutePath();
        }
        return paths;
    }

    private void compressTmpFile (File file, File tmpFile, byte[] buffer, byte[] compressionBuffer) throws IOException {
        int bufferUsed = 0;
        final byte[] crlfBytes = "\r\n".getBytes(charset);
        BufferedReader tmpIn = new BufferedReader(new InputStreamReader(new FileInputStream(tmpFile), charset), 1 << 21);
        FileOutputStream out = new FileOutputStream(file);
        for (String line = tmpIn.readLine(); line != null; line = tmpIn.readLine()) {
            byte[] bytes = line.getBytes(charset);
            if (bufferUsed + bytes.length + crlfBytes.length > buffer.length) {
                compressBlock(out, buffer, compressionBuffer, bufferUsed);
                bufferUsed = 0;
            }
            System.arraycopy(bytes, 0, buffer, bufferUsed, bytes.length);
            System.arraycopy(crlfBytes, 0, buffer, bufferUsed + bytes.length, crlfBytes.length);
            bufferUsed += bytes.length + crlfBytes.length;
        }
        compressBlock(out, buffer, compressionBuffer, bufferUsed);
        tmpIn.close();
        out.flush();
        out.close();
    }
    private void compressBlock (FileOutputStream out, byte[] buffer, byte[] compressionBuffer, int bufferUsed) throws IOException {
        if (compression == CompressionType.SNAPPY) {
            int sizeAfterCompression = Snappy.compress(buffer, 0, bufferUsed, compressionBuffer, 8);
            if (sizeAfterCompression > compressionBuffer.length) {
                // this might happen, but not sure how Snappy-java handles exceptional cases..
                throw new IOException ("compression buffer too small???");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("compressed a block in snappy:" + bufferUsed + " -> " + sizeAfterCompression);
            }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("compressed a block in gzip:" + bufferUsed + " -> " + sizeAfterCompression);
            }
            ByteBuffer.wrap(compressionBuffer).asIntBuffer().put(new int[]{bufferUsed, sizeAfterCompression});
            out.write(compressionBuffer, 0, 8 + sizeAfterCompression);
        } else {
            out.write(buffer, 0, bufferUsed);
        }
    }

    @Override
    public void close() throws IOException {
        partitioner.close();
        if (recusriveTmpFolder.exists()) {
            boolean deleted = recusriveTmpFolder.delete();
            if (!deleted) {
                LOG.warn("couldn't delete temporary folder:" + recusriveTmpFolder.getAbsolutePath());
            }
        }
    }
}
