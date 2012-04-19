package edu.brown.lasvegas.lvfs.data.task;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;
import edu.brown.lasvegas.lvfs.data.PartitionedTextFileWriters;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Sub task of {@link JobType#IMPORT_FRACTURE}.
 * Given one or more text files in the local filesystem (not HDFS),
 * this task partitions them into local temporary files.
 * This one is easier than PARTITION_HDFS_TEXT_FILES because
 * of the record-boundary issue in HDFS text files. 
 * @see TaskType#PARTITION_RAW_TEXT_FILES
 */
public final class PartitionRawTextFilesTaskRunner extends DataTaskRunner<PartitionRawTextFilesTaskParameters> {
    private static Logger LOG = Logger.getLogger(PartitionRawTextFilesTaskRunner.class);

    /** Buffer size to read an input file. */
    public static final String READ_BUFFER_KEY = "lasvegas.server.data.task.partition_raw_text.read_buffer_size";
    public static final int READ_BUFFER_DEFAULT = 1 << 20;

    /** Buffer size to write _each_ partitioned file. */
    public static final String WRITE_BUFFER_KEY = "lasvegas.server.data.task.partition_raw_text.write_buffer_size";
    public static final int WRITE_BUFFER_DEFAULT = 1 << 18;

    /**
     * max number of partitioned files to write at once. When num of partitions is larger than this number, we scan
     * the input file more than once. Notice that we will consume write_buffer_size * write_partitions_max memory.
     */
    public static final String WRITE_PARTITIONS_MAX_KEY = "lasvegas.server.data.task.partition_raw_text.write_partitions_max";
    public static final int WRITE_PARTITIONS_MAX_DEFAULT = 1 << 8;

    private int readBufferSize;
    private int writeBufferSize;
    private int writePartitionsMax;
    private CompressionType compression;
    
    private List<String> outputFilePaths;

    @Override
    protected String[] runDataTask() throws Exception {
        readConf ();
        outputFilePaths = new ArrayList<String>();
        VirtualFile[] inputFiles = new VirtualFile[parameters.getFilePaths().length];
        for (int i = 0; i < parameters.getFilePaths().length; ++i) {
            String path = parameters.getFilePaths()[i];
            File file = new File (path);
            if (!file.exists()) {
                throw new IOException ("this input file doesn't exist:" + file.getAbsolutePath());
            }
            inputFiles[i] = new LocalVirtualFile(file);
        }
        LVFracture fracture = context.metaRepo.getFracture(parameters.getFractureId());
        if (fracture == null) {
            throw new IOException ("this fracture ID doesn't exist:" + parameters.getFractureId());
        }
        LVColumn[] allColumns = context.metaRepo.getAllColumnsExceptEpochColumn(fracture.getTableId());
        LVReplicaGroup[] groups = context.metaRepo.getAllReplicaGroups(fracture.getTableId());

        // partition the files for each replica group
        // TODO partition for all replica groups altogether to speed-up. a bit tricky though.
        for (LVReplicaGroup group : groups) {
            ValueRange[] ranges = group.getRanges();
            int partitions = ranges.length;
            LVColumn partitioningColumn = context.metaRepo.getColumn(group.getPartitioningColumnId());
            LOG.info("partitioning files for replica group:" + group);
            // TODO this code should be refactored to use ValueRange#extractStartKeys
            switch (partitioningColumn.getType()) {
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                partitionFiles (extractStartKeys(Long.MIN_VALUE, ranges, new Long[partitions]),
                            inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            case INTEGER:
                partitionFiles (extractStartKeys(Integer.MIN_VALUE, ranges, new Integer[partitions]),
                                inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            case SMALLINT:
                partitionFiles (extractStartKeys(Short.MIN_VALUE, ranges, new Short[partitions]),
                                inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            case TINYINT:
                partitionFiles (extractStartKeys(Byte.MIN_VALUE, ranges, new Byte[partitions]),
                                inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            case FLOAT:
                partitionFiles (extractStartKeys(Float.MIN_VALUE, ranges, new Float[partitions]),
                                inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            case DOUBLE:
                partitionFiles (extractStartKeys(Double.MIN_VALUE, ranges, new Double[partitions]),
                                inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            case VARCHAR:
                partitionFiles (extractStartKeys("", ranges, new String[partitions]),
                                inputFiles, fracture, group, allColumns, partitioningColumn);
                break;
            default:
                throw new IOException ("unexpected partition column type:" + partitioningColumn);
            }
            LOG.info("partitioning done for the group");
        }
        LOG.info("all partitioning done. " + outputFilePaths.size() + " output files");
        return outputFilePaths.toArray(new String[0]);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> T[] extractStartKeys(T minValue, ValueRange[] ranges, T[] startKeys) {
        int partitions = ranges.length;
        assert (partitions == startKeys.length);
        for (int i = 0; i < partitions; ++i) {
            startKeys[i] = (T) ranges[i].getStartKey();
            if (startKeys[i] == null) startKeys[i] = minValue;
            if (i != 0) {
                assert (startKeys[i - 1].compareTo(startKeys[i]) < 0);
            }
        }
        return startKeys;
    }

    private <T extends Comparable<T>> void partitionFiles (T[] startKeys,
                    VirtualFile[] inputFiles,
                    LVFracture fracture, LVReplicaGroup group,
                    LVColumn[] allColumns, LVColumn partitioningColumn) throws Exception {
        checkTaskCanceled ();
        int partitions = startKeys.length;
        
        boolean[] partitionsCompleted = new boolean[partitions];
        Arrays.fill(partitionsCompleted, false);
        int partitioningColumnIndex = -1;
        assert (!allColumns[0].getName().equals(LVColumn.EPOCH_COLUMN_NAME)); // epoch column should be already ignored
        for (int i = 0; i < allColumns.length; ++i) {
            LVColumn column = allColumns[i];
            if (column.getColumnId() == partitioningColumn.getColumnId()) {
                partitioningColumnIndex = i;
            }
        }
        assert (partitioningColumnIndex >= 0);
        // set columnType only to the partitioning column to bypass parsing other columns.
        // we only need partitioning in this task. also, we ignore columns after the partitioning column
        ColumnType[] dummyColumnTypes = new ColumnType[partitioningColumnIndex + 1];
        dummyColumnTypes[partitioningColumnIndex] = allColumns[partitioningColumnIndex].getType();
        while (true) {
            PartitionedTextFileWriters writers = new PartitionedTextFileWriters(context.localLvfsTmpDir, context.nodeId, group.getGroupId(), fracture.getFractureId(), partitions,
                            partitionsCompleted, parameters.getEncoding(), writeBufferSize, writePartitionsMax, compression);
            try {
                // scan all input files
                TextFileTupleReader reader = new TextFileTupleReader(inputFiles, CompressionType.NONE, dummyColumnTypes, 
                                parameters.getDelimiter(), readBufferSize, Charset.forName(parameters.getEncoding()),
                                new SimpleDateFormat(parameters.getDateFormat()),
                                new SimpleDateFormat(parameters.getTimeFormat()),
                                new SimpleDateFormat(parameters.getTimestampFormat()));
                try {
                    int checkCounter = 0;
                    while (reader.next()) {
                        @SuppressWarnings("unchecked")
                        T partitionValue = (T) reader.getObject(partitioningColumnIndex);
                        int partition = findPartition (partitionValue, startKeys);
                        assert (partition >= 0 && partition < partitions);
                        writers.write(partition, reader.getCurrentTupleAsString());
                        if (++checkCounter % 100000 == 0) {
                            checkTaskCanceled ();
                        }
                    }
                } finally {
                    reader.close();
                }
                String[] paths = writers.complete();
                for (String path : paths) {
                    outputFilePaths.add(path);
                }
                partitionsCompleted = writers.getPartitionCompleted();
                if (!writers.isPartitionRemaining()) {
                    // some partition was skipped to save memory. scan the file again.
                    break;
                }
            } finally {
                writers.close();
            }
        }
    }
    private static <T extends Comparable<T>> int findPartition (T partitionValue, T[] startKeys) {
        int arrayPos = Arrays.binarySearch(startKeys, partitionValue);
        if (arrayPos < 0) {
            // non-exact match. start from previous one
            arrayPos = (-arrayPos) - 1; // this "-1" is binarySearch's design
            arrayPos -= 1; // this -1 means "previous one"
            if (arrayPos == -1) {
                arrayPos = 0;
            }
        }
        if (arrayPos >= startKeys.length) {
            arrayPos = startKeys.length - 1;
        }
        assert (partitionValue.compareTo(startKeys[arrayPos]) >= 0);
        assert (arrayPos > startKeys.length - 2 || partitionValue.compareTo(startKeys[arrayPos + 1]) < 0);
        return arrayPos;
    }

    private void readConf () throws Exception {
        readBufferSize = context.conf.getInt(READ_BUFFER_KEY, READ_BUFFER_DEFAULT);
        writeBufferSize = context.conf.getInt(WRITE_BUFFER_KEY, WRITE_BUFFER_DEFAULT);
        writePartitionsMax = context.conf.getInt(WRITE_PARTITIONS_MAX_KEY, WRITE_PARTITIONS_MAX_DEFAULT);
        compression = parameters.getTemporaryCompression();
        LOG.info("partitioning " + parameters.getFilePaths().length + " files. readBufferSize=" + readBufferSize
                        + ", writeBufferSize=" + writeBufferSize + ", writePartitionsMax=" + writePartitionsMax
                        + ", compression=" + compression);
    }
}
