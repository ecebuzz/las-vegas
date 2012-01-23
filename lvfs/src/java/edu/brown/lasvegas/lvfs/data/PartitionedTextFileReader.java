package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Reads a file written by {@link PartitionedTextFileWriters}.
 * <p>If the file is not compressed, this is merely a BufferedReader, and actually
 * this class works for general text files.
 * If the file is gzip/snappy compressed, this class does block-by-block access.</p>
 * 
 * <p>Unlike PartitionedTextFileWriters, this file might be reading from a remote
 * file. Hence, the constructor receives VirtualFile, not java.io.File.</p>
 */
public class PartitionedTextFileReader implements Closeable {
    private static Logger LOG = Logger.getLogger(PartitionedTextFileReader.class);

    private final int fileLength;
    private final Charset charset;
    private final CompressionType compression;
    private final boolean compressed;
    private InputStream in;
    
    /** only when compression==none.*/
    private BufferedReader plainTextReader;
    
    /** when compression==gzip/snappy.*/
    private String[] currentBlock = new String[0];
    private int currentBlockConsumed = 0;
    private int fileReadBytes = 0;
    
    public PartitionedTextFileReader (VirtualFile file, Charset charset, CompressionType compression, int bufferSize) throws IOException {
        this.fileLength = (int) file.length();
        this.charset = charset;
        this.compression = compression;
        this.in = file.getInputStream();
        this.compressed = (compression == CompressionType.GZIP_BEST_COMPRESSION || compression == CompressionType.SNAPPY);
        if (!compressed) {
            plainTextReader = new BufferedReader(new InputStreamReader(in, charset), bufferSize);
        }
    }
    
    private byte[] header = new byte[8];
    private int[] headerInt = new int[2]; // original size, compressed size
    private boolean getNextBlock () throws IOException {
        assert (fileReadBytes == fileLength || fileReadBytes <= fileLength - 8); // 8 bytes header should always exist
        if (fileReadBytes == fileLength) {
            return false;
        }
        readBytes (header);
        ByteBuffer.wrap(header).asIntBuffer().get(headerInt);

        if (LOG.isDebugEnabled()) {
            LOG.debug("reading compressed text file block. the header says size before compression=" + headerInt[0] + " bytes, after=" + headerInt[1]);
        }
        assert (headerInt[0] >= 0);
        assert (headerInt[0] <= 1 << 26);
        assert (headerInt[1] > 0);
        assert (headerInt[1] <= 1 << 26);
        // read the compressed block at once
        byte[] compressedBuffer = new byte[headerInt[1]];
        readBytes (compressedBuffer);

        LOG.debug("uncompressing...");
        // uncompress it
        byte[] originalBytes = new byte[headerInt[0]];
        if (compression == CompressionType.SNAPPY) {
            int written = Snappy.uncompress(compressedBuffer, 0, compressedBuffer.length, originalBytes, 0);
            assert (written == originalBytes.length);
        } else {
            assert (compression == CompressionType.GZIP_BEST_COMPRESSION);
            GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressedBuffer), 1 << 16);
            int usedTotal = 0;
            while (usedTotal < originalBytes.length) {
                int used = gzip.read(originalBytes, usedTotal, originalBytes.length - usedTotal);
                usedTotal += used;
            }
            assert (usedTotal == originalBytes.length);
            gzip.close();
        }
        
        // then, read it as text lines
        LOG.debug("decoding and line-splitting...");
        String str = new String (originalBytes, charset);
        currentBlock = str.split("\\r?\\n");
        currentBlockConsumed = 0;
        if (LOG.isDebugEnabled()) {
            LOG.debug("read and uncompressed the block. it had " + currentBlock.length + " lines");
        }
        return true;
    }
    private void readBytes (byte[] buf) throws IOException {
        int read = in.read (buf);
        assert (read == buf.length);
        fileReadBytes += read;
    }
    
    /**
     * Returns a next line in this file.
     * @return next line. null if reached the end of file.
     * @throws IOException
     */
    public String readLine () throws IOException {
        if (plainTextReader != null) {
            return plainTextReader.readLine();
        } else {
            while (currentBlockConsumed == currentBlock.length) {
                boolean readSomething = getNextBlock();
                if (!readSomething) {
                    return null; // reached end
                }
            }
            return currentBlock[currentBlockConsumed++];
        }
    }
    
    @Override
    public void close () throws IOException {
        if (plainTextReader != null) {
            plainTextReader.close();
            plainTextReader = null;
        }
        if (in != null) {
            in.close(); // might be double-close, but close() should be fine with it.
            in = null;
        }
    }
}
