package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileWriterBundle;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.TupleBuffer;
import edu.brown.lasvegas.util.MemoryUtil;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Recursively repartitions column files to smaller column files,
 * potentially doing multiple stages of repartitioning.
 * The purpose of this class is to avoid opening too many files
 * (linux's no_file limit) and reduce maximum memory consumption while repartitioning.
 * 
 * Quite similar to {@link RecursiveTextFilePartitioner}, but this class deals with
 * columnar files, not row-oriented text files.
 */
public class RecursiveRepartitioner {
    private static Logger LOG = Logger.getLogger(RecursiveRepartitioner.class);
    /** number of columns. */
    private final int columnCount;

    /** the folder to store all output files. */
    private final VirtualFile outputFolder;
    
    /** existing columnar files. [0 to basePartitions.len-1][0 to columnCount-1]. we read them one by one. */
    private final ColumnFileBundle[][] baseFiles;
    /** used to buffer tuples read from existing columnar files. */
    private final TupleBuffer readBuffer;
    /**
     * writer objects for repartitioned files. [fragment][0 to columnCount-1].
     * We probably hold many writer objects because the existing partitioning and
     * re-partitioning scheme are not correlated.
     */
    private final ColumnFileWriterBundle[][] writers;
    /** files written by the writers. [fragment][0 to columnCount-1]. */
    private final ColumnFileBundle[][] fragmentResults;
    /** temporary folder used by each writer. [fragment]. */
    private final VirtualFile[] fragmentFolders;

    /** type of each column to output. */
    private final ColumnType[] columnTypes;
    /** data type traits for each column. */
    @SuppressWarnings("rawtypes")
    private final ValueTraits[] traits;
    /** how all of new and existing columnar files are compressed. */
    private final CompressionType[] compressions;

    /** the partitioning column. index in the array (0 to columnCount-1). */
    private final int partitioningColumnIndex;
    /**
     * The key ranges of the partitioning column.
     * Sorted by the ranges themselves.
     */
    private final ValueRange[] partitionRanges;
    /** start keys extracted  from partitionRanges. used by binary search to determine the partition.*/
    private final Object partitionStartKeys;

    /**
     * the maximum number of fragments to write out at each level. if num of partitions is larger than this, we recursively repartition
     * to limit the number of open files and memory consumption.
     */ 
    private final int maxFragments;

    /** the range of partitions this partitioner will receive. */
    private final int partitionsBegin, partitionsEnd;
    /** total number of partitions (before fragmenting) _in this range _. */
    private final int partitions;
    /** number of fragments of this level. */
    private final int fragments;
    /** partition / this number = fragment. */
    private final int partitionsPerFragment;

    /**
     * The number of tuples to read at once.
     */
    private final int readCacheTuples;
    /** the byte size of buffer for _all_ column file writers at each level. */
    private final long writeBufferSizeTotal;
    /** the byte size of buffer for _each_ column file writer. */
    private final int writeBufferSize;


    /** overload without partitionsBegin/partitionsEnd for convenience. */
    public RecursiveRepartitioner (VirtualFile outputFolder, ColumnFileBundle[][] baseFiles,
                    ColumnType[] columnTypes, CompressionType[] compressions,
                    int partitioningColumnIndex, ValueRange[] partitionRanges, int maxFragments,
                    int readCacheTuples, long writeBufferSizeTotal) {
        this(outputFolder, baseFiles, columnTypes, compressions, partitioningColumnIndex, partitionRanges,
                        0, partitionRanges.length,
                        maxFragments, readCacheTuples, writeBufferSizeTotal);
    }

    @SuppressWarnings("unchecked")
    public RecursiveRepartitioner (VirtualFile outputFolder, ColumnFileBundle[][] baseFiles,
            ColumnType[] columnTypes, CompressionType[] compressions,
            int partitioningColumnIndex, ValueRange[] partitionRanges,
            int partitionsBegin, int partitionsEnd, int maxFragments,
            int readCacheTuples, long writeBufferSizeTotal) {
        this.outputFolder = outputFolder;
        this.baseFiles = baseFiles;
        this.columnCount = columnTypes.length;
        this.columnTypes = columnTypes;
        this.traits = new ValueTraits<?, ?>[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            traits[i] = ValueTraitsFactory.getInstance(columnTypes[i]);
        }
        this.compressions = compressions;
        
        this.partitioningColumnIndex = partitioningColumnIndex;
        assert (partitioningColumnIndex >= 0 && partitioningColumnIndex < columnCount);
        this.partitionRanges = partitionRanges;
        if (readCacheTuples < 1) {
            throw new IllegalArgumentException("invalid readCacheTuples:" + readCacheTuples);
        }
        this.readCacheTuples = readCacheTuples;
        this.readBuffer = new TupleBuffer(columnTypes, readCacheTuples);
        this.partitionStartKeys = ValueRange.extractStartKeys(traits[partitioningColumnIndex], partitionRanges);

        if (maxFragments <= 1) {
            throw new IllegalArgumentException("invalid maxFragments:" + maxFragments);
        }
        assert (maxFragments > 1);
        assert (partitionsBegin >= 0);
        assert (partitionsBegin < partitionRanges.length);
        // assert (partitionsEnd <= partitionRanges.length); this CAN happen. for example, the upper level's partitionsPerFragment=5. partitions=[0, 13]. the last child will be [10,15]
        assert (partitionsBegin <= partitionsEnd);
        this.maxFragments = maxFragments;
        this.partitionsBegin = partitionsBegin;
        this.partitionsEnd = partitionsEnd;
        this.partitions = partitionsEnd - partitionsBegin;
        
        if (partitions <= maxFragments) {
            this.fragments = partitions;
            this.partitionsPerFragment = 1;
        } else {
            this.fragments = maxFragments;
            this.partitionsPerFragment = (int) Math.ceil((double) partitions / maxFragments);
            assert (partitionsPerFragment > 1);
        }
        this.writers = new ColumnFileWriterBundle[fragments][];
        this.fragmentResults = new ColumnFileBundle[fragments][];
        this.fragmentFolders = new VirtualFile[fragments];

        if (writeBufferSizeTotal < fragments * columnCount * 10) {
            throw new IllegalArgumentException("invalid writeBufferSizeTotal:" + writeBufferSizeTotal);
        }
        this.writeBufferSizeTotal = writeBufferSizeTotal;
        final int MAX_WRITE_BUFFER_SIZE_PER_FRAGMENT = 1 << 24;
        if ((int) (writeBufferSizeTotal / fragments) > MAX_WRITE_BUFFER_SIZE_PER_FRAGMENT) {
            this.writeBufferSize = MAX_WRITE_BUFFER_SIZE_PER_FRAGMENT / columnCount;
        } else {
            this.writeBufferSize = (int) (writeBufferSizeTotal / fragments) / columnCount;
        }
    }
    /**
     * Repartitions the given columnar files.
     * @return the repartitioned columnar files.
     * These objects are temporary objects that are not registered to the repository (thus no ID assigned).
     * The resulting columnar files are stored in a recursive way: <outputFolder>/fragment_xxx/fragment_yyy/.../<column> + extensions.
     */
    public ColumnFileBundle[][] execute () throws IOException {
        repartitionOneLevel ();

        // now, we might have to recurse the repartitioning 
        if (partitionsPerFragment > 1) {
            // then we have to recurse
            ColumnFileBundle[][] recursedResults = new ColumnFileBundle[partitions][];
            for (int fragment = 0; fragment < fragments; ++fragment) {
                if (fragmentResults[fragment] == null) {
                    continue;
                }
                LOG.info("recurse: " + (partitionsBegin + partitionsPerFragment * fragment) + " to " + (partitionsBegin + partitionsPerFragment * (fragment + 1)));
                final int recursedPartitionsBegin = partitionsBegin + partitionsPerFragment * fragment;
                RecursiveRepartitioner recursedPartitioner = new RecursiveRepartitioner(
                                fragmentFolders[fragment],
                                new ColumnFileBundle[][]{fragmentResults[fragment]},
                                columnTypes, compressions, partitioningColumnIndex, partitionRanges,
                                recursedPartitionsBegin,
                                recursedPartitionsBegin + partitionsPerFragment,
                                maxFragments, readCacheTuples, writeBufferSizeTotal);
                ColumnFileBundle[][] recursed = recursedPartitioner.execute();
                assert (recursed.length == partitionsPerFragment);
                for (int i = 0; i < recursed.length; ++i) {
                    if (recursed[i] == null) {
                        continue;
                    }
                    assert (partitionsPerFragment * fragment + i < partitions);
                    recursedResults[partitionsPerFragment * fragment + i] = recursed[i];
                }
            }
            return recursedResults;
        } else {
            LOG.info("was last level");
            assert (fragmentResults.length == partitions);
            return fragmentResults;
        }
    }
    /** read all tuples from the given files and write them out to fragment writers. the results are stored in fragmentResults. */
    private void repartitionOneLevel () throws IOException {
        LOG.info("started. outputting available memory...");
        MemoryUtil.outputMemory();

        try {
            for (ColumnFileBundle[] files : baseFiles) {
                assert (files.length == columnCount);
                ColumnFileTupleReader reader = new ColumnFileTupleReader(files, readCacheTuples * 8);// the stream cache size is totally heuristic (but probably not too off)
                try {
                    consumeReaders (reader);
                } finally {
                    reader.close();
                }
            }
        } catch (IOException e) {
            LOG.error("observed an exception in Repartitioner. re-throwing", e);
            throw e;
        } finally {
            for (int fragment = 0; fragment < fragments; ++fragment) {
                if (writers[fragment] == null) {
                    continue;
                }
                fragmentResults[fragment] = new ColumnFileBundle[columnCount];
                for (int j = 0; j < columnCount; ++j) {
                    ColumnFileWriterBundle writer = writers[fragment][j];
                    if (writer == null) {
                        LOG.warn("writer[" + fragment + "][" + j + "] hasn't been initialized. wtf? probably some other exception occurred?");
                        continue;
                    }
                    writer.finish();
                    writer.close();
                    fragmentResults[fragment][j] = new ColumnFileBundle(writer, false); // not sorted 
                    writers[fragment][j] = null; //help GC
                }
                writers[fragment] = null; //help GC
            }
        }
        LOG.info("done this level. outputting available memory...");
        MemoryUtil.outputMemory();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void consumeReaders (ColumnFileTupleReader reader) throws IOException {
        LOG.info("reading data from readers....");
        while (true) {
            readBuffer.resetCount();
            int read = reader.nextBatch(readBuffer);
            if (read < 0) {
                break;
            }
            Object[] data = new Object[columnCount];
            for (int i = 0; i < columnCount; ++i) {
                data[i] = readBuffer.getColumnBuffer(i);
            }
            for (int i = 0; i < read; ++i) {
                Comparable partitionValue = traits[partitioningColumnIndex].get(data[partitioningColumnIndex], i);
                int partition = ValueRange.findPartition(traits[partitioningColumnIndex], partitionValue, partitionStartKeys);
                assert (partition >= partitionsBegin && partition < partitionsEnd);
                int fragment = (partition - partitionsBegin) / partitionsPerFragment;
                assureWriter (fragment);

                // The code below could use a TupleBuffer (size of outputCacheSize) to avoid wrapper object creation.
                // But, as noted in the class comment, CPU overhead is not the primary concern here.
                // rather, we avoid consuming more RAM. (that's one of the purposes to do this recursive stuff)
                for (int j = 0; j < columnCount; ++j) {
                    ((TypedWriter) writers[fragment][j].getDataWriter()).writeValue(traits[j].get(data[j], i));
                }
            }
        }
    }
    
    private void assureWriter (int fragment) throws IOException {
        if (writers[fragment] != null) {
            return;
        }
        // this fragment is first found, so let's create a new writer.
        VirtualFile folder = outputFolder.getChildFile("fragment_" + fragment);
        folder.mkdirs();
        if (!folder.exists()) {
            throw new IOException ("failed to create a temporary folder to store repartitioned files. " + folder.getAbsolutePath());
        }

        fragmentFolders[fragment] = folder;
        writers[fragment] = new ColumnFileWriterBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            writers[fragment][i] = new ColumnFileWriterBundle(folder, String.valueOf(i), columnTypes[i], compressions[i], true, writeBufferSize);
        }
    }
}
