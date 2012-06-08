package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.data.task.RepartitionTaskRunner;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
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

    /** the folder to store all output files. */
    private final LocalVirtualFile outputFolder;
    /** a temporary folder to store intermediate files. */
    private final LocalVirtualFile recursiveOutputFolder;
    
    private final int columnCount;
    private final int partitions;

    /** core implementation of this object. */
    private final RecursiveRepartitioner impl;

    /**
     * @param outputFolder the folder to store all output files. All columnar files are named <outputFolder>/<partition>/<column> + extensions.
     * @param baseFiles files to repartition. [arbitrary][columns]
     * @param columnTypes
     * @param compressions
     * @param partitioningColumnIndex
     * @param partitionRanges
     * @param maxFragments the maximum number of fragments to write out at each level. if num of partitions is larger than this, we recursively repartition
     * to limit the number of open files and memory consumption. 
     * @param readCacheTuples The number of tuples to read at once.
     * @param writeBufferSizeTotal the byte size of buffer for _all_ column file writers at each level
     * @throws IOException
     */
	public Repartitioner (LocalVirtualFile outputFolder, ColumnFileBundle[][] baseFiles,
            ColumnType[] columnTypes, CompressionType[] compressions,
            int partitioningColumnIndex, ValueRange[] partitionRanges,
            int maxFragments, int readCacheTuples, long writeBufferSizeTotal) throws IOException {
    	this.outputFolder = outputFolder;
    	this.recursiveOutputFolder = outputFolder.getChildFile("recursive_tmp_" + Math.abs( new Random (System.nanoTime()).nextInt()));
    	recursiveOutputFolder.mkdirs();
    	if (!recursiveOutputFolder.exists()) {
    	    throw new IOException ("couldn't make a temporary folder: " + recursiveOutputFolder.getAbsolutePath());
    	}
    	this.columnCount = columnTypes.length;
    	this.partitions = partitionRanges.length;
    	this.impl = new RecursiveRepartitioner(recursiveOutputFolder,
    	                baseFiles, columnTypes, compressions, partitioningColumnIndex, partitionRanges,
    	                maxFragments, readCacheTuples, writeBufferSizeTotal);
    }

    /**
     * Repartitions the given files.
     * @return descriptors of the repartitioned columnar files.
     * These LVColumnFile objects are temporary objects that are not registered to the repository (thus no ID assigned).
     * All columnar files are named <outputFolder>/<partition>/<column> + extensions.
     */
    public LVColumnFile[][] execute () throws IOException {
        // just call the implementation object
        ColumnFileBundle[][] results = impl.execute();
        assert (results.length == partitions);

        // the only thing done here is to convert ColumnFileBundle into LVColumnFile.
        LVColumnFile[][] finalResults = new LVColumnFile[partitions][];
        for (int partition = 0; partition < partitions; ++partition) {
            if (results[partition] == null) {
                continue;
            }
            finalResults[partition] = new LVColumnFile[columnCount];
            for (int column = 0; column < columnCount; ++column) {
                ColumnFileBundle file = results[partition][column];
                // move the repartitioned files to outputFolder
                file.moveFiles(outputFolder.getChildFile(String.valueOf(partition)), String.valueOf(column));
                // then, convert the object into LVColumnFile
                finalResults[partition][column] = file.toLVColumnFile();
            }
        }
        
        // ... and to delete intermediate files (recursively repartitioned files. the first level is already deleted by the file move above).
        boolean deleted = recursiveOutputFolder.delete(true);
        if (!deleted) {
            LOG.warn("some temporary file(s) couldn't be deleted: " + recursiveOutputFolder);
        }
        return finalResults;
    }
    
 }

