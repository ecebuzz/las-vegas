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

import org.apache.log4j.Logger;

/**
 * A set of writers to write out temporary partitioned text files.
 * In order to limit memory usage, this class recursively partitions
 * the given file.
 */
public final class RecursiveTextFilePartitioner implements Closeable {
    private static Logger LOG = Logger.getLogger(RecursiveTextFilePartitioner.class);
    
    /**
     * @param maxFragments Number of FragmentWriter in each level. If the total number of partitions is
     * larger than this value, we recursively repartition.
     * In other words, fan-out.
     */
    public RecursiveTextFilePartitioner (File outputDir, int maxFragments,
            int partitionsBegin, int partitionsEnd, Charset charset,
            long writeBufferSizeTotal) {
        this.outputDir = outputDir;
        if (!outputDir.exists()) {
            boolean created = outputDir.mkdirs();
            assert (created);
        }
        this.maxFragments = maxFragments;
        this.partitionsBegin = partitionsBegin;
        this.partitionsEnd = partitionsEnd;
        this.partitions = partitionsEnd - partitionsBegin;
        
        // map partitions (may be 1000s) to fragments (at most maxFragments).
        // if #partitions>maxFragments, we should always have maxFragments fragments
        // because then we have to re-read smaller portions of the file later.
        // also, we want to evenly distribute partitions to fragments, so...
        if (partitions <= maxFragments) {
            this.fragments = partitions;
            this.partitionsPerFragment = 1;
        } else {
            this.fragments = maxFragments;
            this.partitionsPerFragment = (int) Math.ceil((double) partitions / maxFragments);
            assert (partitionsPerFragment > 1);
        }
        this.writers = new FragmentWriter[fragments];
        
        this.charset = charset;
        this.writeBufferSizeTotal = writeBufferSizeTotal;
        final int MAX_WRITE_BUFFER_SIZE = 1 << 24;
        if ((int) (writeBufferSizeTotal / fragments) > MAX_WRITE_BUFFER_SIZE) {
            this.writeBufferSize = MAX_WRITE_BUFFER_SIZE;
        } else {
            this.writeBufferSize = (int) (writeBufferSizeTotal / fragments);
        }
        this.crlfBytes = "\r\n".getBytes(charset);
    }
    /** parent folder to place the output file(s). */
    private final File outputDir;
    private final int maxFragments;

    /** the range of partitions this partitioner will receive. */
    private final int partitionsBegin, partitionsEnd;
    /** total number of partitions. */
    private final int partitions;
    /** number of fragments of this level. */
    private final int fragments;
    /** index=fragment. */
    private final FragmentWriter[] writers;
    /** partition / this number = fragment. */
    private final int partitionsPerFragment;
    
    private final byte[] crlfBytes;
    

    private final Charset charset;
    private final long writeBufferSizeTotal;
    private final int writeBufferSize;

    /** Write one tuple to the corresponding partition file. */
    public void write (int partition, String line) throws IOException {
        assert (partition >= partitionsBegin);
        assert (partition < partitionsEnd);
        int fragment = (partition - partitionsBegin) / partitionsPerFragment;
        assert (fragment < fragments);
        if (writers[fragment] == null) {
            File folder = new File (outputDir, "fragment_" + fragment);
            writers[fragment] = new FragmentWriter(folder);
        }
        writers[fragment].write (partition, line);
    }
    
    /**
     * Close all writers and report the resulting file paths.
     * If needed, this method recurses to further partition fragments to partitions.
     * @return full path of resulting files. index=partition
     */
    public String[] complete () throws IOException {
        String[] paths = new String[partitions];
        // first, finish each fragment writer
        for (int i = 0; i < fragments; ++i) {
            FragmentWriter writer = writers[i];
            if (writer == null) {
                continue;
            }
            writer.close();
        }

        // then, recursively partition each fragment, if needed.
        int[] partitionBuffer = new int[1 << 20];
        byte[] partitionBufferConverted = new byte[partitionBuffer.length * 4];
        for (int fragment = 0; fragment < fragments; ++fragment) {
            FragmentWriter writer = writers[fragment];
            if (writer == null) {
                continue;
            }
            if (partitionsPerFragment > 1 && writer.defaultPartition < 0) {
                // then we have to recurse
                LOG.info("recurse: " + (partitionsBegin + partitionsPerFragment * fragment) + " to " + (partitionsBegin + partitionsPerFragment * (fragment + 1)));
                RecursiveTextFilePartitioner recursedPartitioner = new RecursiveTextFilePartitioner(writer.folder, maxFragments,
                                partitionsBegin + partitionsPerFragment * fragment, partitionsBegin + partitionsPerFragment * (fragment + 1),
                                charset, writeBufferSizeTotal);
                BufferedReader in = new BufferedReader (new InputStreamReader(new FileInputStream(writer.file), charset), 1 << 22);
                FileInputStream partIn = new FileInputStream(writer.partFile);
                while (true) {
                    int partBufferBytes = partIn.read(partitionBufferConverted, 0, partitionBufferConverted.length);
                    if (partBufferBytes < 0) {
                        break;
                    }
                    assert (partBufferBytes % 4 == 0);
                    int partTuples = partBufferBytes / 4;
                    ByteBuffer.wrap(partitionBufferConverted).asIntBuffer().get(partitionBuffer, 0, partTuples);
                    for (int j = 0; j < partTuples; ++j) {
                        int partition = partitionBuffer[j];
                        String line = in.readLine();
                        assert (line != null);
                        recursedPartitioner.write(partition, line);
                    }
                }
                partIn.close();
                in.close();
                String[] recursedResults = recursedPartitioner.complete();
                assert (recursedResults.length == partitionsPerFragment);
                for (int j = 0; j < recursedResults.length; ++j) {
                    if (recursedResults[j] == null) {
                        continue; // this is fine. the partition had no tuple.
                    }
                    File movedFile = new File(outputDir, "partition_" + (recursedPartitioner.partitionsBegin + j));
                    boolean moved = new File(recursedResults[j]).renameTo(movedFile);
                    assert (moved);
                    paths[recursedPartitioner.partitionsBegin + j - partitionsBegin] = movedFile.getAbsolutePath();
                }
                recursedPartitioner.close(); // to delete all temporary files. note that we do this AFTER we move the partitioned file above 
            } else {
                // we don't have to partition any more
                assert (partitionsPerFragment > 1 || writer.defaultPartition == partitionsBegin + fragment);
                int partition;
                if (partitionsPerFragment == 1) {
                    LOG.info("was last level");
                    partition = partitionsBegin + fragment;
                } else {
                    LOG.info("was not last level, but contained only one partition:" + writer.defaultPartition);
                    partition = writer.defaultPartition;
                }
                assert (partition >= partitionsBegin);
                assert (partition < partitionsEnd);
                File movedFile = new File(outputDir, "partition_" + partition);
                boolean moved = writer.file.renameTo(movedFile);
                assert (moved);
                paths[partition - partitionsBegin] = movedFile.getAbsolutePath();
            }
        }
        return paths;
    }
    @Override
    public void close() throws IOException {
        for (int i = 0; i < writers.length; ++i) {
            FragmentWriter writer = writers[i];
            if (writer == null) {
                continue;
            }
            writer.close();

            // delete temporary files
            if (writer.file.exists()) {
                boolean deleted = writer.file.delete();
                assert (deleted);
            }
            if (writer.partFile.exists()) {
                boolean deleted = writer.partFile.delete();
                assert (deleted);
            }
            if (writer.folder.exists()) {
                boolean deleted = writer.folder.delete();
                assert (deleted);
            }
            writers[i] = null;
        }
    }
    /** writes out intermediate repartitioned file (fragment). */
    public class FragmentWriter {
        private final static int MAX_BUFFERED_TUPLES = 1 << 14;
        public FragmentWriter(File folder) throws IOException {
            this.folder = folder;
            if (!folder.exists()) {
                boolean created = folder.mkdirs();
                assert (created);
            }
            this.buffer = new byte[writeBufferSize];
            this.partitionBuffer = new int[MAX_BUFFERED_TUPLES];
            this.partitionBufferConverted = new byte[MAX_BUFFERED_TUPLES * 4];
            this.file = new File(folder, "data");
            this.out = new FileOutputStream(file, false);
            this.partFile = new File(folder, "partitions");
            this.partOut = new FileOutputStream(partFile, false);
        }
        private final File folder, file, partFile;
        private final FileOutputStream out, partOut;
        private final byte[] buffer;
        private final int[] partitionBuffer;
        private final byte[] partitionBufferConverted;

        private int bufferUsed = 0;
        private int bufferedLines = 0;
        private boolean firstTuple = true;
        /** if this fragment so far received only one partition, the partition number. Otherwise, -1, meaning this fragment contains more than one partitions. */
        private int defaultPartition = 0;

        public void write (int partition, String line) throws IOException {
            byte[] bytes = line.getBytes(charset);
            if (bufferedLines == MAX_BUFFERED_TUPLES || bufferUsed + bytes.length + crlfBytes.length > buffer.length) {
                flushBlock ();
            }
            if (firstTuple) {
                firstTuple = false;
                defaultPartition = partition;
            } else {
                if (defaultPartition != partition) {
                    defaultPartition = -1;
                }
            }
            System.arraycopy(bytes, 0, buffer, bufferUsed, bytes.length);
            System.arraycopy(crlfBytes, 0, buffer, bufferUsed + bytes.length, crlfBytes.length);
            bufferUsed += bytes.length + crlfBytes.length;
            partitionBuffer[bufferedLines] = partition;
            ++bufferedLines;
        }
        private void flushBlock () throws IOException {
            if (bufferUsed == 0) {
                return;
            }
            // flush current buffer as a new block. at this point we don't compress because this file will be read again in this node shortly.
            out.write(buffer, 0, bufferUsed);
            // write out partition numbers too.
            ByteBuffer.wrap(partitionBufferConverted).asIntBuffer().put(partitionBuffer, 0, bufferedLines);
            partOut.write(partitionBufferConverted, 0, bufferedLines * 4);
            
            bufferUsed = 0;
            bufferedLines = 0;
        }
        public void close () throws IOException {
            flushBlock();
            out.flush();
            out.close();
            partOut.flush();
            partOut.close();
        }
    }
}
