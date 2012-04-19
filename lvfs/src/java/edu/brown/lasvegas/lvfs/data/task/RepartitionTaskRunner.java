package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;
import edu.brown.lasvegas.lvfs.data.Repartitioner;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;

/**
 * Output partitioned columnar files for the given partitioning column and range (which
 * is probably different from the current partitioning column/range).
 * @see TaskType#REPARTITION
 */
public final class RepartitionTaskRunner extends DataTaskRunner<RepartitionTaskParameters> {
    private static Logger LOG = Logger.getLogger(RepartitionTaskRunner.class);
    
    private LVColumn[] columns;
    private int partitioningColumnIndex;
    private ColumnType[] columnTypes;
    private CompressionType[] compressions;
    private LVReplicaPartition[] basePartitions;
    private ColumnFileBundle[][] baseFiles;
    private VirtualFile tmpFolder;
    private VirtualFile tmpOutputFolder;
    
    @Override
    protected String[] runDataTask() throws Exception {
        LOG.info("repartitioning " + parameters.getBasePartitionIds().length + " partitions..");
        prepareInputs ();
        checkTaskCanceled();
        
        Repartitioner repartitioner = new Repartitioner(tmpOutputFolder, baseFiles, columnTypes, compressions,
        		partitioningColumnIndex, parameters.getPartitionRanges(),
        		parameters.getReadCacheSize(), parameters.getOutputCacheSize());
        Set<Integer> partitions = repartitioner.execute();
        LOG.info("done!");
        
        // the result is a list of partition folders
        ArrayList<String> ret = new ArrayList<String>();
        for (Integer partition : partitions) {
        	ret.add(tmpOutputFolder.getAbsolutePath() + "/" + partition);
        }
        return ret.toArray(new String[0]);
    }

    private void prepareInputs () throws Exception {
        if (parameters.getOutputColumnIds().length != parameters.getOutputCompressions().length) {
            throw new IOException ("lengthes of output column ID/compression arrays don't match");
        }
        if (parameters.getOutputColumnIds().length == 0) {
            throw new IOException ("no output column specified");
        }
        columns = new LVColumn[parameters.getOutputColumnIds().length];
        columnTypes = new ColumnType[columns.length];
        compressions = new CompressionType[columns.length];
        partitioningColumnIndex = -1;
        for (int i = 0; i < columns.length; ++i) {
        	columns[i] = context.metaRepo.getColumn(parameters.getOutputColumnIds()[i]);
            columnTypes[i] = columns[i].getType();
            compressions[i] = parameters.getOutputCompressions()[i];

            // get the index of partitioning column in the array. note that it's not the ID of LVColumn!
        	if (columns[i].getColumnId() == parameters.getPartitioningColumnId()) {
        		partitioningColumnIndex = i;
        	}
        }
	    if (partitioningColumnIndex == -1) {
	        throw new IOException ("the partitioning column " + parameters.getPartitioningColumnId() + " wasn't found in the output column list");
	    }
	    assert (partitioningColumnIndex >=0 && partitioningColumnIndex < columns.length);
        
        basePartitions = new LVReplicaPartition[parameters.getBasePartitionIds().length];
        baseFiles = new ColumnFileBundle[basePartitions.length][columns.length];
        for (int i = 0; i < parameters.getBasePartitionIds().length; ++i) {
            int id = parameters.getBasePartitionIds()[i];
            basePartitions[i] = context.metaRepo.getReplicaPartition(id);
            if (basePartitions[i] == null) {
                throw new IOException ("this partition ID doesn't exist:" + id);
            }
            if (basePartitions[i].getNodeId().intValue() != context.nodeId) {
                throw new IOException ("this partition isn't local:" + id);
            }
            for (int j = 0; j < columns.length; ++j) {
	            LVColumnFile baseFile = context.metaRepo.getColumnFileByReplicaPartitionAndColumn(basePartitions[i].getPartitionId(), columns[j].getColumnId());
                baseFiles[i][j] = new ColumnFileBundle(baseFile);
            }
        }

        tmpFolder = new LocalVirtualFile (context.localLvfsTmpDir);
        tmpOutputFolder = tmpFolder.getChildFile("repartition_tmp_" + Math.abs(new Random(System.nanoTime()).nextInt()));
        tmpOutputFolder.mkdirs();
        if (!tmpOutputFolder.exists()) {
            throw new IOException ("failed to create a temporary output folder: " + tmpOutputFolder.getAbsolutePath());
        }
    }
}
