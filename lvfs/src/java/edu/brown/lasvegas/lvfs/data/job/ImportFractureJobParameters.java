package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobParameters;

/**
 * Set of parameters for one data import.
 * This specifies what files to import, to which table, etc.
 */
public final class ImportFractureJobParameters extends JobParameters {
    /**
     * The table to which a new fracture is constructed during this import.
     */
    private int tableId;
    
    /** Encoding name of the imported files. */
    private String encoding;

    /** Column delimiter. */
    private char delimiter;

    /**
     * the format string to parse a date column in the files.
     * @see SimpleDateFormat
     */
    private String dateFormat;
    
    /**
     * the format string to parse a time column in the files.
     * @see SimpleDateFormat
     */
    private String timeFormat;
    
    /**
     * the format string to parse a timestamp column in the files.
     * @see SimpleDateFormat
     */
    private String timestampFormat;
    
    /**
     * Whether (and which) to use compression for temporary files while data import.
     * only none/snappy/gzip. default is snappy.
     */
    private CompressionType temporaryFileCompression = CompressionType.SNAPPY;

    /** the files to import in each data node. key = ID of node (LVNode), value = local paths. */
    private Map<Integer, String[]> nodeFilePathMap = new HashMap<Integer, String[]>();
    
    /**
     * Instantiates a new data import parameters.
     */
    public ImportFractureJobParameters () {}
    
    /**
     * Instantiates a new data import parameters.
     * @param tableId The table to which a new fracture is constructed during this import.
     */
    public ImportFractureJobParameters (int tableId) {
        this (tableId, "UTF-8", '|', "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", CompressionType.SNAPPY);
    }
    /**
     * Instantiates a new data import parameters.
     * @param tableId The table to which a new fracture is constructed during this import.
     * @param encoding Encoding name of the imported files, eg, "UTF-8"
     * @param delimiter Column delimiter, eg, '|'
     * @param dateFormat the format string to parse a date column in the files, eg, "yyyy-MM-dd"
     * @param timeFormat the format string to parse a time column in the files, eg, "HH:mm:ss"
     * @param timestampFormat the format string to parse a timestamp column in the files, eg, "yyyy-MM-dd HH:mm:ss.SSS"
     */
    public ImportFractureJobParameters (int tableId, String encoding, char delimiter,
                    String dateFormat, String timeFormat, String timestampFormat, CompressionType temporaryFileCompression) {
        this.tableId = tableId;
        this.encoding = encoding;
        Charset.forName(encoding); // test if the encoding exists (at least in this JavaVM)
        this.delimiter = delimiter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.temporaryFileCompression = temporaryFileCompression;
        assert (temporaryFileCompression == CompressionType.GZIP_BEST_COMPRESSION
                || temporaryFileCompression == CompressionType.NONE
                || temporaryFileCompression == CompressionType.SNAPPY);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        tableId = in.readInt();
        encoding = readNillableString(in);
        delimiter = in.readChar();
        dateFormat = readNillableString(in);
        timeFormat = readNillableString(in);
        timestampFormat = readNillableString(in);
        temporaryFileCompression = CompressionType.values()[in.readInt()];

        nodeFilePathMap.clear();
        int entries = in.readInt();
        for (int i = 0; i < entries; ++i) {
            int nodeId = in.readInt();
            int stringCount = in.readInt();
            String[] array = new String[stringCount];
            for (int j = 0; j < array.length; ++j) {
                array[j] = readNillableString(in);
            }
            assert (!nodeFilePathMap.containsKey(nodeId));
            nodeFilePathMap.put(nodeId, array);
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tableId);
        writeNillableString(out, encoding);
        out.writeChar(delimiter);
        writeNillableString(out, dateFormat);
        writeNillableString(out, timeFormat);
        writeNillableString(out, timestampFormat);
        out.writeInt(temporaryFileCompression == null ? CompressionType.INVALID.ordinal() : temporaryFileCompression.ordinal());

        out.writeInt(nodeFilePathMap.size());
        for (Map.Entry<Integer, String[]> entry : nodeFilePathMap.entrySet()) {
            out.writeInt(entry.getKey());
            String[] array = entry.getValue();
            out.writeInt(array.length);
            for (int i = 0; i < array.length; ++i) {
                writeNillableString(out, array[i]);
            }
        }
    }
    

    /**
     * Gets the ID of the table to which a new fracture is constructed during this import.
     *
     * @param ID of the table to which a new fracture is constructed during this import.
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Sets the ID of the table to which a new fracture is constructed during this import.
     *
     * @param tableId the table to which a new fracture is constructed during this import.
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the format string to parse a date column in the files.
     *
     * @return the format string to parse a date column in the files
     */
    public String getDateFormat() {
        return dateFormat;
    }

    /**
     * Sets the format string to parse a date column in the files.
     *
     * @param dateFormat the new format string to parse a date column in the files
     */
    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    /**
     * Gets the format string to parse a time column in the files.
     *
     * @return the format string to parse a time column in the files
     */
    public String getTimeFormat() {
        return timeFormat;
    }

    /**
     * Sets the format string to parse a time column in the files.
     *
     * @param timeFormat the new format string to parse a time column in the files
     */
    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    /**
     * Gets the format string to parse a timestamp column in the files.
     *
     * @return the format string to parse a timestamp column in the files
     */
    public String getTimestampFormat() {
        return timestampFormat;
    }

    /**
     * Sets the format string to parse a timestamp column in the files.
     *
     * @param timestampFormat the new format string to parse a timestamp column in the files
     */
    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    /**
     * Gets the encoding name of the imported files.
     *
     * @return the encoding name of the imported files
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Sets the encoding name of the imported files.
     *
     * @param encoding the new encoding name of the imported files
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Gets the column delimiter.
     *
     * @return the column delimiter
     */
    public char getDelimiter() {
        return delimiter;
    }

    /**
     * Sets the column delimiter.
     *
     * @param delimiter the new column delimiter
     */
    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * Gets the files to import in each data node.
     *
     * @return the files to import in each data node
     */
    public Map<Integer, String[]> getNodeFilePathMap() {
        return nodeFilePathMap;
    }

    /**
     * Sets the files to import in each data node.
     *
     * @param nodeFilePathMap the new files to import in each data node
     */
    public void setNodeFilePathMap(Map<Integer, String[]> nodeFilePathMap) {
        this.nodeFilePathMap = nodeFilePathMap;
    }

    /**
     * Gets the whether (and which) to use compression for temporary files while data import.
     *
     * @return the whether (and which) to use compression for temporary files while data import
     */
    public CompressionType getTemporaryFileCompression() {
        return temporaryFileCompression;
    }

    /**
     * Sets the whether (and which) to use compression for temporary files while data import.
     *
     * @param temporaryFileCompression the new whether (and which) to use compression for temporary files while data import
     */
    public void setTemporaryFileCompression(CompressionType temporaryFileCompression) {
        this.temporaryFileCompression = temporaryFileCompression;
    }
}
