package edu.brown.lasvegas.lvfs.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVTask;

/**
 * Common parameters for data import from text files. 
 */
public abstract class TextFileImportTaskParameters extends DataTaskParameters {
    public TextFileImportTaskParameters() {
        super();
    }
    public TextFileImportTaskParameters(byte[] serializedParameters) throws IOException {
        super(serializedParameters);
    }
    public TextFileImportTaskParameters(LVTask task) throws IOException {
        super(task);
    }
    
    /**
     * The fracture to be constructed after this import.
     */
    private int fractureId;
    
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
     * which compression scheme to use for compressing temporary files. snappy/gzip/none.
     * When a temporary file is compressed, its extension becomes like .gz, .snappy.
     * Also, each compressed block starts from two 4-byte integers
     * which tells the sizes of the compressed block before/after compression.
     */
    private CompressionType temporaryCompression;
    
    @Override
    public final void write(DataOutput out) throws IOException {
        out.writeInt(fractureId);
        out.writeUTF(encoding);
        out.writeChar(delimiter);
        out.writeUTF(dateFormat);
        out.writeUTF(timeFormat);
        out.writeUTF(timestampFormat);
        out.writeInt(temporaryCompression == null ? CompressionType.INVALID.ordinal() : temporaryCompression.ordinal());
        writeDerived (out);
    }

    /** override this to serialize more attributes. */
    protected abstract void writeDerived (DataOutput out) throws IOException;
    
    @Override
    public final void readFields(DataInput in) throws IOException {
        fractureId = in.readInt();
        encoding = in.readUTF();
        delimiter = in.readChar();
        dateFormat = in.readUTF();
        timeFormat = in.readUTF();
        timestampFormat = in.readUTF();
        temporaryCompression = CompressionType.values()[in.readInt()];
        readFieldsDerived (in);
    }
    /** override this to de-serialize more attributes. */
    protected abstract void readFieldsDerived(DataInput in) throws IOException;
    
    // auto-generated getters/setters (comments by JAutodoc)    
    /**
     * Gets the fracture to be constructed after this import.
     *
     * @return the fracture to be constructed after this import
     */
    public final int getFractureId() {
        return fractureId;
    }
    
    /**
     * Sets the fracture to be constructed after this import.
     *
     * @param fractureId the new fracture to be constructed after this import
     */
    public final void setFractureId(int fractureId) {
        this.fractureId = fractureId;
    }
    
    /**
     * Gets the encoding name of the imported files.
     *
     * @return the encoding name of the imported files
     */
    public final String getEncoding() {
        return encoding;
    }
    
    /**
     * Sets the encoding name of the imported files.
     *
     * @param encoding the new encoding name of the imported files
     */
    public final void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    
    /**
     * Gets the column delimiter.
     *
     * @return the column delimiter
     */
    public final char getDelimiter() {
        return delimiter;
    }
    
    /**
     * Sets the column delimiter.
     *
     * @param delimiter the new column delimiter
     */
    public final void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }
    
    /**
     * Gets the format string to parse a date column in the files.
     *
     * @return the format string to parse a date column in the files
     */
    public final String getDateFormat() {
        return dateFormat;
    }
    
    /**
     * Sets the format string to parse a date column in the files.
     *
     * @param dateFormat the new format string to parse a date column in the files
     */
    public final void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
    
    /**
     * Gets the format string to parse a time column in the files.
     *
     * @return the format string to parse a time column in the files
     */
    public final String getTimeFormat() {
        return timeFormat;
    }
    
    /**
     * Sets the format string to parse a time column in the files.
     *
     * @param timeFormat the new format string to parse a time column in the files
     */
    public final void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }
    
    /**
     * Gets the format string to parse a timestamp column in the files.
     *
     * @return the format string to parse a timestamp column in the files
     */
    public final String getTimestampFormat() {
        return timestampFormat;
    }
    
    /**
     * Sets the format string to parse a timestamp column in the files.
     *
     * @param timestampFormat the new format string to parse a timestamp column in the files
     */
    public final void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    /**
     * Gets the compression for the temporary files.
     *
     * @return the compression for the temporary files
     */
    public final CompressionType getTemporaryCompression() {
        return temporaryCompression;
    }

    /**
     * Sets the compression for the temporary files.
     *
     * @param temporaryCompression the new compression for the temporary files
     */
    public final void setTemporaryCompression(CompressionType temporaryCompression) {
        this.temporaryCompression = temporaryCompression;
    }
}
