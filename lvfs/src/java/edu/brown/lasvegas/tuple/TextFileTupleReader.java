package edu.brown.lasvegas.tuple;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.Arrays;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.ValueRun;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.PartitionedTextFileReader;
import edu.brown.lasvegas.util.ByteArray;

/**
 * A tuple reader implementation which reads one or more text files.
 * As this reader is not backed by a columnar file, some methods are not implemented or slow.
 * It simply reads and returns line by line although that's the best bet on text file import..
 */
public class TextFileTupleReader extends DefaultTupleReader {
    private final VirtualFile[] textFiles;
    private final CompressionType textFileCompression;

    private final int columnCount;
    private final ColumnType[] columnTypes;
    private final int buffersize;
    private final Charset charset;
    private final String delimiter;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    private PartitionedTextFileReader reader;
    private final Object[] currentData;

    /** index in #textFiles. */
    private int nextTextFile = 0;
    /** grand total from the first file. */
    private int currentTuple = 0;

    public TextFileTupleReader (
        VirtualFile[] textFiles, CompressionType textFileCompression,
        ColumnType[] columnTypes, String delimiter, int buffersize, Charset charset,
        DateFormat dateFormat, DateFormat timeFormat, DateFormat timestampFormat)
    throws IOException {
        this.textFiles = textFiles;
        this.textFileCompression = textFileCompression;
        this.columnTypes = columnTypes;
        this.columnCount = columnTypes.length;
        this.delimiter = delimiter;
        this.buffersize = buffersize;
        this.charset = charset;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.currentData = new Object[columnCount];
    }

    private void closeCurrentFile () throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
    private boolean openNextFile () throws IOException {
        closeCurrentFile();
        if (nextTextFile >= textFiles.length) {
            return false; // no more files
        }
        reader = new PartitionedTextFileReader(textFiles[nextTextFile], charset, textFileCompression, buffersize);
        return true;
    }

    @Override
    public void close() throws IOException {
        closeCurrentFile();
    }

    @Override
    public void seekToTupleAbsolute(int tuple) throws IOException {
        if (tuple == 0) {
            // only supports going back to the first tuple.
            closeCurrentFile();
            nextTextFile = 0;
            currentTuple = 0;
            openNextFile();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean next() throws IOException {
        String line;
        while (true) {
            line = reader.readLine();
            if (line == null) {
                Arrays.fill(currentData, null);
                return false;
            }
            if (line.length() == 0) {
                continue;
            }
            break;
        }
        String[] stringData = line.split(delimiter);
        if (stringData.length < columnCount) {
            throw new IOException ("invalid line " + currentTuple + ". column count doesn't match:" + line);
        }
        try {
            for (int i = 0; i < columnCount; ++i) {
                switch (columnTypes[i]) {
                case BIGINT: currentData[i] = Long.valueOf(stringData[i]); break;
                case BOOLEAN: currentData[i] = Boolean.valueOf(stringData[i]); break;
                case DOUBLE: currentData[i] = Double.valueOf(stringData[i]); break;
                case FLOAT: currentData[i] = Float.valueOf(stringData[i]); break;
                case INTEGER: currentData[i] = Integer.valueOf(stringData[i]); break;
                case SMALLINT: currentData[i] = Short.valueOf(stringData[i]); break;
                case TINYINT: currentData[i] = Byte.valueOf(stringData[i]); break;

                case DATE: currentData[i] = dateFormat.parse(stringData[i]).getTime(); break;
                case TIME: currentData[i] = timeFormat.parse(stringData[i]).getTime(); break;
                case TIMESTAMP: currentData[i] = timestampFormat.parse(stringData[i]).getTime(); break;
                }
            }
        } catch (Exception ex) {
            throw new IOException ("invalid line " + currentTuple + ". failed to parse:" + line, ex);
        }
        ++currentTuple;
        return true;
    }

    @Override
    public int getCurrentTuple() throws IOException {
        return currentTuple;
    }

    @Override
    public int getTupleCount() throws IOException {
        // text file reader can't give 
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public ColumnType getColumnType(int columnIndex) {
        return columnTypes[columnIndex];
    }

    @Override
    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    @Override
    public CompressionType getCompressionType(int columnIndex) {
        return CompressionType.NONE;
    }

    @Override
    public CompressionType[] getCompressionTypes() {
        CompressionType[] dummy = new CompressionType[columnCount];
        Arrays.fill(dummy, CompressionType.NONE);
        return dummy;
    }

    @Override
    public OrderedDictionary<?, ?> getDictionary(int columnIndex) {
        return null;
    }

    @Override
    public int getDictionaryCompressedValuesByte(int columnIndex, byte[] buffer, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDictionaryCompressedValuesShort(int columnIndex, short[] buffer, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDictionaryCompressedValuesInt(int columnIndex, int[] buffer, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueRun<?> getCurrentRun(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex) throws IOException {
        return currentData[columnIndex];
    }

    @Override
    public boolean getBoolean(int columnIndex) throws IOException {
        return (Boolean) currentData[columnIndex];
    }

    @Override
    public byte getTinyint(int columnIndex) throws IOException {
        return (Byte) currentData[columnIndex];
    }

    @Override
    public short getSmallint(int columnIndex) throws IOException {
        return (Short) currentData[columnIndex];
    }

    @Override
    public int getInteger(int columnIndex) throws IOException {
        return (Integer) currentData[columnIndex];
    }

    @Override
    public long getBigint(int columnIndex) throws IOException {
        return (Long) currentData[columnIndex];
    }

    @Override
    public float getFloat(int columnIndex) throws IOException {
        return (Float) currentData[columnIndex];
    }

    @Override
    public double getDouble(int columnIndex) throws IOException {
        return (Double) currentData[columnIndex];
    }

    @Override
    public String getVarchar(int columnIndex) throws IOException {
        return (String) currentData[columnIndex];
    }

    @Override
    public ByteArray getVarbin(int columnIndex) throws IOException {
        return (ByteArray) currentData[columnIndex];
    }
}
