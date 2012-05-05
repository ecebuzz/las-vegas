package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileWriterBundle;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskRunner;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.TupleBuffer;
import edu.brown.lasvegas.util.MemoryUtil;
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
     * @return descriptors of the repartitioned columnar files.
     * These LVColumnFile objects are temporary objects that are not registered to the repository (thus no ID assigned).
     * All columnar files are named <outputFolder>/<partition>/<column> + extensions.
     */
    public LVColumnFile[][] execute () throws IOException {
        LOG.info("started. outputting available memory...");
        MemoryUtil.outputMemory();
        LVColumnFile[][] ret = new LVColumnFile[partitionRanges.length][];
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
        } catch (IOException e) {
        	LOG.error("observed an exception in Repartitioner. re-throwing", e);
        	throw e;
        } finally {
        	for (int i = 0; i < partitionRanges.length; ++i) {
        		if (writers[i] == null) {
        			continue;
        		}
        		ret[i] = new LVColumnFile[columnCount];
            	for (int j = 0; j < columnCount; ++j) {
            		ColumnFileWriterBundle writer = writers[i][j];
            		if (writer == null) {
            			LOG.warn("writer[" + i + "][" + j + "] hasn't been initialized. wtf? probably some other exception occurred?");
            			continue;
            		}
            		writer.finish();
            		writer.close();
            		LVColumnFile result = new LVColumnFile();
            		result.setChecksum(writer.getDataFileChecksum());
            		result.setColumnType(columnTypes[j]);
            		result.setCompressionType(compressions[j]);
            		result.setDictionaryBytesPerEntry(writer.getDictionaryBytesPerEntry());
            		result.setDistinctValues(writer.getDistinctValues());
            		result.setFileSize((int) writer.getDataFile().length());
            		result.setLocalFilePath(outputFolder.getAbsolutePath() + "/" + i + "/" + j);
            		result.setRunCount(writer.getRunCount());
            		result.setSorted(false);
            		result.setTupleCount(writer.getTupleCount());
            		if (writer.getTupleCount() == 0) {
                		result.setTupleCount(writer.getTupleCount());
            		}
            		result.setUncompressedSizeKB(writer.getUncompressedSizeKB());
            		ret[i][j] = result;
            		writers[i][j] = null;
            	}
        		writers[i] = null;
        	}
        }
        LOG.info("done.");
        return ret;
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
		folder.mkdirs();
		if (!folder.exists()) {
			throw new IOException ("failed to create a temporary folder to store repartitioned files. " + folder.getAbsolutePath());
		}

		writers[partition] = new ColumnFileWriterBundle[columnCount];
		for (int i = 0; i < columnCount; ++i) {
			writers[partition][i] = new ColumnFileWriterBundle(folder, String.valueOf(i), columnTypes[i], compressions[i], true,
					1 << 13); // to avoid OutofMemory, uses only 8kb buffer.
		}
		
		if (partition % 20 == 0) {
	        LOG.info("checking available memory...");
	        MemoryUtil.outputMemory();
		}
	}
	
	
	//public static ColumnFileTupleReader openRepartitionedFiles (VirtualFile outputFolder) throws IOException {
		
	//}
 }

