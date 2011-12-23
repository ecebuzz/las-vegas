package edu.brown.lasvegas.lvfs.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskParameters;

/**
 * Parameters for {@link PartitionRawTextFilesTask}.
 */
public final class PartitionRawTextFilesTaskParameters extends TaskParameters {
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

    /** the local path of files to import in this data node. */
    private String[] filePaths;

    
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
        out.writeInt(filePaths.length);
        for (String path : filePaths) {
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
        filePaths = new String[in.readInt()];
        for (int i = 0; i < filePaths.length; ++i) {
            filePaths[i] = in.readUTF();
        }
    }

    public PartitionRawTextFilesTaskParameters() {
        super();
    }
    public PartitionRawTextFilesTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public PartitionRawTextFilesTaskParameters(LVTask task) throws IOException {
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
     * Gets the local path of files to import in this data node.
     *
     * @return the local path of files to import in this data node
     */
    public String[] getFilePaths() {
        return filePaths;
    }
    
    /**
     * Sets the local path of files to import in this data node.
     *
     * @param filePaths the new local path of files to import in this data node
     */
    public void setFilePaths(String[] filePaths) {
        this.filePaths = filePaths;
    }
}