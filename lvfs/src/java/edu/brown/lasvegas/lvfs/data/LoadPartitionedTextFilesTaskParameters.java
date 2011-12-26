package edu.brown.lasvegas.lvfs.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import edu.brown.lasvegas.LVTask;

/**
 * Parameters for {@link LoadPartitionedTextFilesTaskRunner}.
 */
public final class LoadPartitionedTextFilesTaskParameters extends DataTaskParameters {
    /**
     * The fracture to be constructed after this import.
     */
    private int fractureId;
    
    /** Encoding name of the imported files. */
    private String encoding;

    /** Column delimiter. */
    private String delimiter;

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
     * path of temporary partitioned files at each node.
     * The file names will tell their replica group ID, partitions, and Node ID.
     */
    private String[] temporaryPartitionedFiles;
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fractureId);
        out.writeUTF(encoding);
        out.writeUTF(delimiter);
        out.writeUTF(dateFormat);
        out.writeUTF(timeFormat);
        out.writeUTF(timestampFormat);
        out.writeInt(temporaryPartitionedFiles.length);
        for (String path : temporaryPartitionedFiles) {
            out.writeUTF(path);
        }
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        fractureId = in.readInt();
        encoding = in.readUTF();
        delimiter = in.readUTF();
        dateFormat = in.readUTF();
        timeFormat = in.readUTF();
        timestampFormat = in.readUTF();
        temporaryPartitionedFiles = new String[in.readInt()];
        for (int i = 0; i < temporaryPartitionedFiles.length; ++i) {
            temporaryPartitionedFiles[i] = in.readUTF();
        }
    }
    
    public LoadPartitionedTextFilesTaskParameters() {
        super();
    }
    public LoadPartitionedTextFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public LoadPartitionedTextFilesTaskParameters(LVTask task) throws IOException {
        super(task);
    }
    
    // auto-generated getters/setters (comments by JAutodoc)    
    /**
     * Gets the fracture to be constructed after this import.
     *
     * @return the fracture to be constructed after this import
     */
    public int getFractureId() {
        return fractureId;
    }
    
    /**
     * Sets the fracture to be constructed after this import.
     *
     * @param fractureId the new fracture to be constructed after this import
     */
    public void setFractureId(int fractureId) {
        this.fractureId = fractureId;
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
    public String getDelimiter() {
        return delimiter;
    }
    
    /**
     * Sets the column delimiter.
     *
     * @param delimiter the new column delimiter
     */
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
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
     * Gets the path of temporary partitioned files at each node.
     *
     * @return the path of temporary partitioned files at each node
     */
    public String[] getTemporaryPartitionedFiles() {
        return temporaryPartitionedFiles;
    }
    
    /**
     * Sets the path of temporary partitioned files at each node.
     *
     * @param temporaryPartitionedFiles the new path of temporary partitioned files at each node
     */
    public void setTemporaryPartitionedFiles(String[] temporaryPartitionedFiles) {
        this.temporaryPartitionedFiles = temporaryPartitionedFiles;
    }
}
