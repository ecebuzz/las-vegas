package edu.brown.lasvegas.lvfs.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVSubPartitionScheme;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.imp.TextFileTableReader;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
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

    /**
     * which compression scheme to use for compressing the partitioned files. snappy/gzip/none.
     * When the file is compressed, its extension becomes like .gz, .snappy.
     * Also, each compressed block (write_buffer_size after de-compression) starts from two 4-byte integers
     * which tells the sizes of the compressed block before/after compression.
     */
    public static final String COMPRESS_KEY = "lasvegas.server.data.task.partition_raw_text.compression";
    public static final String COMPRESS_DEFAULT = "snappy";
    
    private int readBufferSize;
    private int writeBufferSize;
    private int writePartitionsMax;
    private CompressionType compression;
    
    private List<String> outputFilePaths;

    @Override
    protected String[] runDataTask() throws Exception {
        readConf ();
        outputFilePaths = new ArrayList<String>();
        List<LocalVirtualFile> inputFiles = new ArrayList<LocalVirtualFile>();
        for (String path : parameters.getFilePaths()) {
            File file = new File (path);
            if (!file.exists()) {
                throw new IOException ("this input file doesn't exist:" + file.getAbsolutePath());
            }
            inputFiles.add(new LocalVirtualFile(file));
        }
        LVFracture fracture = context.metaRepo.getFracture(parameters.getFractureId());
        if (fracture == null) {
            throw new IOException ("this fracture ID doesn't exist:" + parameters.getFractureId());
        }
        LVReplicaGroup[] groups = context.metaRepo.getAllReplicaGroups(fracture.getTableId());

        // partition the files for each replica group
        for (LVReplicaGroup group : groups) {
            LVSubPartitionScheme partitionScheme = context.metaRepo.getSubPartitionSchemeByFractureAndGroup(fracture.getFractureId(), group.getGroupId());
            if (partitionScheme == null) {
                throw new IOException ("this fracture and group don't have a corresponding partition scheme yet:" + fracture + "," + group);
            }
            ValueRange<?>[] ranges = partitionScheme.getRanges();
            int partitions = ranges.length;
            LVColumn partitioningColumn = context.metaRepo.getColumn(group.getPartitioningColumnId());
            LOG.info("partitioning files for replica group:" + group);
            switch (partitioningColumn.getType()) {
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:{
                Long[] startKeys = new Long[partitions];
                for (int i = 0; i < partitions; ++i) {
                    startKeys[i] = (Long) ranges[i].getStartKey();
                    if (startKeys[i] == null) startKeys[i] = Long.MIN_VALUE;
                }
                partitionFiles (startKeys, inputFiles, fracture, group, ranges, partitioningColumn);
                break;}
            case INTEGER:{
                Integer[] startKeys = new Integer[partitions];
                for (int i = 0; i < partitions; ++i) {
                    startKeys[i] = (Integer) ranges[i].getStartKey();
                    if (startKeys[i] == null) startKeys[i] = Integer.MIN_VALUE;
                }
                partitionFiles (startKeys, inputFiles, fracture, group, ranges, partitioningColumn);
                break;}
            case SMALLINT:{
                Short[] startKeys = new Short[partitions];
                for (int i = 0; i < partitions; ++i) {
                    startKeys[i] = (Short) ranges[i].getStartKey();
                    if (startKeys[i] == null) startKeys[i] = Short.MIN_VALUE;
                }
                partitionFiles (startKeys, inputFiles, fracture, group, ranges, partitioningColumn);
                break;}
            case TINYINT:{
                Byte[] startKeys = new Byte[partitions];
                for (int i = 0; i < partitions; ++i) {
                    startKeys[i] = (Byte) ranges[i].getStartKey();
                    if (startKeys[i] == null) startKeys[i] = Byte.MIN_VALUE;
                }
                partitionFiles (startKeys, inputFiles, fracture, group, ranges, partitioningColumn);
                break;}
            case VARCHAR:{
                String[] startKeys = new String[partitions];
                for (int i = 0; i < partitions; ++i) {
                    startKeys[i] = (String) ranges[i].getStartKey();
                    if (startKeys[i] == null) startKeys[i] = "";
                }
                partitionFiles (startKeys, inputFiles, fracture, group, ranges, partitioningColumn);
                break;}
            default:
                throw new IOException ("unexpected partition column type:" + partitioningColumn);
            }
            LOG.info("partitioning done for the group");
        }
        LOG.info("all partitioning done. " + outputFilePaths.size() + " output files");
        return outputFilePaths.toArray(new String[0]);
    }
    private <T extends Comparable<T>> void partitionFiles (T[] startKeys,
                    List<LocalVirtualFile> inputFiles,
                    LVFracture fracture, LVReplicaGroup group,
                    ValueRange<?>[] ranges, LVColumn partitioningColumn) throws IOException {
        int partitions = ranges.length;
        for (int i = 1; i < partitions; ++i) {
            assert (startKeys[i - 1].compareTo(startKeys[i]) < 0);
        }
        
        boolean[] partitionsCompleted = new boolean[partitions];
        Arrays.fill(partitionsCompleted, false);
        int partitioningColumnIndex = partitioningColumn.getOrder();
        while (true) {
            PartitionedTextFileWriters writers = new PartitionedTextFileWriters(context.localLvfsTmpDir, context.nodeId, group, fracture, partitions,
                            partitionsCompleted, parameters.getEncoding(), writeBufferSize, writePartitionsMax, compression);
            // scan all input files
            for (LocalVirtualFile file : inputFiles) {
                TextFileTableReader reader = new TextFileTableReader(file, null,
                    parameters.getDelimiter(), readBufferSize, Charset.forName(parameters.getEncoding()),
                    new SimpleDateFormat(parameters.getDateFormat()),
                    new SimpleDateFormat(parameters.getTimeFormat()),
                    new SimpleDateFormat(parameters.getTimestampFormat()));
                while (reader.next()) {
                    @SuppressWarnings("unchecked")
                    T partitionValue = (T) reader.getObject(partitioningColumnIndex);
                    int partition = findPartition (partitionValue, startKeys);
                    writers.write(partition, reader.getCurrentLineString());
                }
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
        String compressionName = context.conf.get(COMPRESS_KEY, COMPRESS_DEFAULT);
        if (compressionName.equalsIgnoreCase("snappy")) {
            compression = CompressionType.SNAPPY;
        } else if (compressionName.equalsIgnoreCase("gzip") || compressionName.equalsIgnoreCase("GZIP_BEST_COMPRESSION")) {
            compression = CompressionType.GZIP_BEST_COMPRESSION;
        } else if (compressionName.length() == 0 || compressionName.equalsIgnoreCase("none")) {
            compression = CompressionType.NONE;
        } else {
            throw new Exception ("invalid value of " + COMPRESS_KEY + ":" + compressionName
                            + ". valid values are: snappy, gzip, none");
        }
        LOG.info("partitioning " + parameters.getFilePaths().length + " files. readBufferSize=" + readBufferSize
                        + ", writeBufferSize=" + writeBufferSize + ", writePartitionsMax=" + writePartitionsMax
                        + ", compression=" + compression);
    }
}
