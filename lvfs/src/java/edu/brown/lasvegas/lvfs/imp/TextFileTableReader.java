package edu.brown.lasvegas.lvfs.imp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.StringTokenizer;

import edu.brown.lasvegas.LVColumnType;
import edu.brown.lasvegas.LVTableReader;

/**
 * An implementation of LVTableReader for a simple
 * line-delimiter and column-delimiter format, such as CSV/TSV.
 */
public class TextFileTableReader implements LVTableReader {
    private final TextFileTableScheme scheme;
    private final String delimiter;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    private final BufferedReader reader;
    private final String[] columnData;
    private long linesRead;

    /** shortcut constructor for SSB/TPCH tbl files. */
    public TextFileTableReader (InputStream in, TextFileTableScheme scheme, String delimiter) {
        this(in, scheme, delimiter, 1 << 20, Charset.forName("UTF-8"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }
    /**
     * Fill constructor.
     * @param in file to read.
     * @param scheme scheme of the table
     * @param delimiter character(s) to tokenize columns
     * @param buffersize buffer size given to underlying BufferedReader
     * @param charset encoding of the file
     * @param dateFormat used to parse a date column
     * @param timeFormat used to parse a time column
     * @param timestampFormat used to parse a timestamp column
     */
    public TextFileTableReader (InputStream in, TextFileTableScheme scheme, String delimiter, int buffersize, Charset charset,
            DateFormat dateFormat, DateFormat timeFormat, DateFormat timestampFormat) {
        this.scheme = scheme;
        this.delimiter = delimiter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        columnData = new String[scheme.getColumnCount()]; 
        reader = new BufferedReader (new InputStreamReader(in, charset), buffersize);
        Arrays.fill(columnData, null);
        linesRead = 0;
    }

    @Override
    public boolean next() throws IOException {
        String line = reader.readLine();
        try {
            if (line == null || line.length() == 0) {
                return false;
            }

            StringTokenizer tokenizer = new StringTokenizer(line, delimiter);
            for (int i = 0; i < scheme.getColumnCount(); ++i) {
                if (!tokenizer.hasMoreTokens()) {
                    return false;
                }
                columnData[i] = tokenizer.nextToken();
            }
            ++linesRead;
            return true;
        } catch (Exception ex) {
            throw new IOException ("falied to parse line " + (linesRead + 1) + ":" + line, ex);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public int getColumnCount() {
        return scheme.getColumnCount();
    }
    @Override
    public LVColumnType getColumnType(int columnIndex) {
        return scheme.getColumnType(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws IOException {
        switch (scheme.getColumnType(columnIndex)) {
        case BIGINT: return Long.valueOf(columnData[columnIndex]);
        case BOOLEAN: return Boolean.valueOf(columnData[columnIndex]);
        case DOUBLE: return Double.valueOf(columnData[columnIndex]);
        case FLOAT: return Float.valueOf(columnData[columnIndex]);
        case INTEGER: return Integer.valueOf(columnData[columnIndex]);
        case SMALLINT: return Short.valueOf(columnData[columnIndex]);
        case TINYINT: return Byte.valueOf(columnData[columnIndex]);

        case DATE: return getSqlDate(columnIndex);
        case TIME: return getSqlTime(columnIndex);
        case TIMESTAMP: return getSqlTimestamp(columnIndex);        
        }
        return columnData[columnIndex];
    }

    @Override
    public boolean wasNull() {
        // in text file format, NULL is not a possible input
        return false;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws IOException {
        return Boolean.parseBoolean(columnData[columnIndex]);
    }

    @Override
    public byte getByte(int columnIndex) throws IOException {
        return Byte.parseByte(columnData[columnIndex]);
    }

    @Override
    public short getShort(int columnIndex) throws IOException {
        return Short.parseShort(columnData[columnIndex]);
    }

    @Override
    public int getInt(int columnIndex) throws IOException {
        return Integer.parseInt(columnData[columnIndex]);
    }

    @Override
    public long getLong(int columnIndex) throws IOException {
        return Long.parseLong(columnData[columnIndex]);
    }

    @Override
    public float getFloat(int columnIndex) throws IOException {
        return Float.parseFloat(columnData[columnIndex]);
    }

    @Override
    public double getDouble(int columnIndex) throws IOException {
        return Double.parseDouble(columnData[columnIndex]);
    }

    @Override
    public String getString(int columnIndex) throws IOException {
        return columnData[columnIndex];
    }

    @Override
    public java.sql.Date getSqlDate(int columnIndex) throws IOException {
        String str = columnData[columnIndex];
        try {
            java.util.Date date = dateFormat.parse(str);
            return new java.sql.Date(date.getTime());
        } catch (ParseException ex) {
            throw new IOException ("falied to parse date string in line " + linesRead + ":" + str, ex);
        }
    }

    @Override
    public java.sql.Time getSqlTime(int columnIndex) throws IOException {
        String str = columnData[columnIndex];
        try {
            java.util.Date date = timeFormat.parse(str);
            return new java.sql.Time(date.getTime());
        } catch (ParseException ex) {
            throw new IOException ("falied to parse time string in line " + linesRead + ":" + str, ex);
        }
    }

    @Override
    public java.sql.Timestamp getSqlTimestamp(int columnIndex) throws IOException {
        String str = columnData[columnIndex];
        try {
            java.util.Date date = timestampFormat.parse(str);
            return new java.sql.Timestamp(date.getTime());
        } catch (ParseException ex) {
            throw new IOException ("falied to parse timestamp string in line " + linesRead + ":" + str, ex);
        }
    }
}
