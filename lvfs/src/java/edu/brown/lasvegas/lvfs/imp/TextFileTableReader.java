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

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.tuple.InputTableReader;

/**
 * An implementation of LVTableReader for a simple
 * line-delimiter and column-delimiter format, such as CSV/TSV.
 */
public class TextFileTableReader implements InputTableReader {
    private final VirtualFile file;
    private final TextFileTableScheme scheme;
    private final int buffersize;
    private final Charset charset;
    private final String delimiter;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    private InputStream in;
    private BufferedReader reader;
    private final String[] columnData;
    private String currentLineString;

    private long linesRead;

    /** shortcut constructor for SSB/TPCH tbl files. */
    public TextFileTableReader (VirtualFile file, TextFileTableScheme scheme, String delimiter) throws IOException {
        this(file, scheme, delimiter, 1 << 20, Charset.forName("UTF-8"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }
    /**
     * Full constructor.
     * @param in file to read.
     * @param scheme scheme of the table
     * @param delimiter character(s) to tokenize columns
     * @param buffersize buffer size given to underlying BufferedReader
     * @param charset encoding of the file
     * @param dateFormat used to parse a date column
     * @param timeFormat used to parse a time column
     * @param timestampFormat used to parse a timestamp column
     */
    public TextFileTableReader (VirtualFile file, TextFileTableScheme scheme, String delimiter, int buffersize, Charset charset,
            DateFormat dateFormat, DateFormat timeFormat, DateFormat timestampFormat) throws IOException {
        this.file = file;
        this.scheme = scheme;
        this.delimiter = delimiter;
        this.buffersize = buffersize;
        this.charset = charset;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        columnData = new String[scheme.getColumnCount()];
        in = file.getInputStream();
        reader = new BufferedReader (new InputStreamReader(in, charset), buffersize);
        Arrays.fill(columnData, null);
        linesRead = 0;
    }
    
    @Override
    public void reset() throws IOException {
        linesRead = 0;
        reader.close();
        Arrays.fill(columnData, null);
        in = file.getInputStream();
        reader = new BufferedReader (new InputStreamReader(in, charset), buffersize);
    }

    @Override
    public boolean next() throws IOException {
        String line = reader.readLine();
        currentLineString = line;
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
    public long length() throws IOException {
        return file.length();
    }
    
    @Override
    public void seekApproximate(long position) throws IOException {
        reset();
        in.skip(position);
        // skip to next tuple
        byte[] ln = "\n".getBytes(charset);
        if (ln.length == 1) {
            // usually here
            while (true) {
                int i = in.read();
                if (i == ln[0] || i < 0) {
                    break;
                }
            }
        } else {
            //UTF-16 and others
            assert (ln.length == 2);
            boolean prev_match = false;
            while (true) {
                int i = in.read();
                if (i < 0) {
                    break;
                }
                if (i == ln[0]) {
                    prev_match = true;
                } else {
                    if (i == ln[1] && prev_match) {
                        break;
                    }
                    prev_match = false;
                }
            }
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
    public ColumnType getColumnType(int columnIndex) {
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

        case DATE: return getSqlDate(columnIndex).getTime();
        case TIME: return getSqlTime(columnIndex).getTime();
        case TIMESTAMP: return getSqlTimestamp(columnIndex).getTime();
        }
        return columnData[columnIndex];
    }

    @Override
    public boolean wasNull() {
        // in text file format, NULL is not a possible input
        return false;
    }
    
    @Override
    public int getCurrentTupleByteSize () {
        return currentLineString.getBytes(charset).length + 1;
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
        switch (scheme.getColumnType(columnIndex)) {
        case DATE: return getSqlDate(columnIndex).getTime();
        case TIME: return getSqlTime(columnIndex).getTime();
        case TIMESTAMP: return getSqlTimestamp(columnIndex).getTime();
        default:
        return Long.parseLong(columnData[columnIndex]);
        }
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

    @Override
    public String toString() {
        return "TextFileTableReader:" + file.getAbsolutePath();
    }

    public String getCurrentLineString() {
        return currentLineString;
    }
}
