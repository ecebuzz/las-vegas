package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileWriterBundle;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskRunner;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.TupleBuffer;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Core implementation of partition merging ({@link RepartitionTaskRunner}).
 * Separated for better modularization and thus ease of testing.
 * Repartition is a very costly operation at its best, so this class doesn't
 * pay too much effort to reduce CPU overhead. Anyway massive read/write disk I/O
 * is the killer.
 */
public final class Repartitioner {
    private static Logger LOG = Logger.getLogger(Repartitioner.class);
    /** number of columns. */
    private final int columnCount;

    /**
     * the folder to store all output files.
     * All columnar files are named <outputFolder>/<partition>/<column ordinal> + extensions.
     */
    private final VirtualFile outputFolder;
    
    /** existing columnar files. [0 to basePartitions.len-1][0 to columnCount-1]. we read them one by one. */
    private final ColumnFileBundle[][] baseFiles;
    /** used to buffer tuples read from existing columnar files. */
    private final TupleBuffer readBuffer;
    /**
     * writer objects for repartitioned files. [partition][0 to columnCount-1].
     * We probably hold many writer objects because the existing partitioning and
     * re-partitioning scheme are not correlated.
     */
    private final ColumnFileWriterBundle[][] writers;

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
     * The number of tuples to read at once.
     */
    private final int readCacheSize;

    /**
     * The number of tuples to write at once for each output partition.
     */
    @SuppressWarnings("unused")
	private final int outputCacheSize;

    @SuppressWarnings("unchecked")
	public Repartitioner (VirtualFile outputFolder, ColumnFileBundle[][] baseFiles,
            ColumnType[] columnTypes, CompressionType[] compressions,
            int partitioningColumnIndex, ValueRange[] partitionRanges,
            int readCacheSize, int outputCacheSize) {
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
    	this.readCacheSize = readCacheSize;
    	this.outputCacheSize = outputCacheSize;
    	this.readBuffer = new TupleBuffer(columnTypes, readCacheSize);
    	this.writers = new ColumnFileWriterBundle[partitionRanges.length][];
    	this.partitionStartKeys = ValueRange.extractStartKeys(traits[partitioningColumnIndex], partitionRanges);
    }

    /**
     * Repartitions 
     * @return the ordinals of output partitions (indexes in partition ranges).
     * All columnar files are named <outputFolder>/<partition>/<column> + extensions.
     */
    public Set<Integer> execute () throws IOException {
        LOG.info("started");
        Set<Integer> outputPartitions = new TreeSet<Integer>();
        try {
            /** reader object for existing columnar files. */
        	for (ColumnFileBundle[] files : baseFiles) {
        		assert (files.length == columnCount);
	            ColumnFileTupleReader reader = new ColumnFileTupleReader(files, readCacheSize * 8);// the stream cache size is totally heuristic (but probably not too off)
	            try {
	            	consumeReaders (reader);
	            } finally {
	        		reader.close();
	            }
        	}
        } finally {
        	for (int i = 0; i < partitionRanges.length; ++i) {
        		if (writers[i] == null) {
        			continue;
        		}
            	for (int j = 0; j < columnCount; ++j) {
            		writers[i][j].finish();
            		writers[i][j].close();
            		writers[i][j] = null;
            	}
        		writers[i] = null;
        	}
        }
        LOG.info("done. output " + outputPartitions.size() + " partitions");
        return outputPartitions;
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
    			assert (partition >= 0 && partition < partitionRanges.length);
    			assureWriter (partition);
    			// TODO this should use a TupleBuffer (size of outputCacheSize) to avoid wrapper object creation.
    			// But, as noted in the class comment, CPU overhead is not the primary concern here.
        		for (int j = 0; j < columnCount; ++j) {
        			((TypedWriter) writers[partition][j].getDataWriter()).writeValue(traits[j].get(data[j], i));
        		}
    		}
    	}
    }
    
	private void assureWriter (int partition) throws IOException {
		if (writers[partition] != null) {
			return;
		}
		// this partition is first found, so let's create a new writer.
		VirtualFile folder = outputFolder.getChildFile(String.valueOf(partition));
		boolean created = folder.mkdirs();
		if (!created) {
			throw new IOException ("failed to create a temporary folder to store repartitioned files. " + folder.getAbsolutePath());
		}

		writers[partition] = new ColumnFileWriterBundle[columnCount];
		for (int i = 0; i < columnCount; ++i) {
			writers[partition][i] = new ColumnFileWriterBundle(folder, String.valueOf(i), columnTypes[i], compressions[i], true);
		}
	}
 }

