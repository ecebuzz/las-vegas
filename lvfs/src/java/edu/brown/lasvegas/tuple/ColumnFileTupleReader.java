package edu.brown.lasvegas.tuple;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.util.ByteArray;

/**
 * Implementation of {@link TupleReader} backed by columnar files.
 * This class is convenient to read tuples from columnar files, but
 * doesn't allow working on compressed values.
 * Hence, directly using the raw column file readers is sometimes beneficial for the best performance.
 * 
 * Anyways, use {@link #nextBatch(TupleBuffer)} for better performance.
 */
public final class ColumnFileTupleReader implements TupleReader {
    private final int columnCount;
    private final ColumnType[] columnTypes;
    private final ColumnFileReaderBundle[] readerBundles;

    /** readers to read the de-compressed values. */
    @SuppressWarnings("rawtypes")
    private final TypedReader[] dataReaders;

    private final int tupleCount;
    private int nextTuplePos;
    
    /** for non-buffered read. */
    private final Object[] currentTuple;

    public ColumnFileTupleReader (ColumnFileBundle[] files) throws IOException {
        this (files, 1 << 16);
    }
    /**
     * @param streamBufferSize buffer size for each underlying columnar file.
     */
    public ColumnFileTupleReader (ColumnFileBundle[] files, int streamBufferSize) throws IOException {
        this.columnCount = files.length;
        this.readerBundles = new ColumnFileReaderBundle[columnCount];
        this.dataReaders = new TypedReader[columnCount];
        this.columnTypes = new ColumnType[columnCount];
        this.currentTuple = new Object[columnCount];
        this.tupleCount = files[0].getTupleCount();
        this.nextTuplePos = 0;
        for (int i = 0; i < columnCount; ++i) {
            assert (tupleCount == files[i].getTupleCount());
            columnTypes[i] = files[i].getColumnType();
            readerBundles[i] = new ColumnFileReaderBundle(files[i], streamBufferSize);
            dataReaders[i] = readerBundles[i].getDataReader();
        }
    }

    /** Returns the number of tuples in these columnar files. */
    public int getTupleCount () {
        return tupleCount;
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnCount; ++i) {
            readerBundles[i].close();
        }
    }
    @Override
    public boolean next() throws IOException {
        if (nextTuplePos >= tupleCount) {
            return false;
        }
        for (int i = 0; i < columnCount; ++i) {
            currentTuple[i] = dataReaders[i].readValue();
        }
        ++nextTuplePos;
        return true;
    }
    @Override
    public int nextBatch(TupleBuffer buffer) throws IOException {
        int read = buffer.appendTuples(dataReaders);
        nextTuplePos += read;
        return read;
    }
    @Override
    public String getCurrentTupleAsString() {
        StringBuffer str = new StringBuffer(128);
        for (int i = 0; i < columnCount; ++i) {
            if (i != 0) {
                str.append("|");
            }
            str.append(currentTuple[i]);
        }
        return new String(str);
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
    public Object getObject(int columnIndex) throws IOException {
        return currentTuple[columnIndex];
    }
    @Override
    public boolean getBoolean(int columnIndex) throws IOException {
        return (Boolean) currentTuple[columnIndex];
    }
    @Override
    public byte getTinyint(int columnIndex) throws IOException {
        return (Byte) currentTuple[columnIndex];
    }
    @Override
    public short getSmallint(int columnIndex) throws IOException {
        return (Short) currentTuple[columnIndex];
    }
    @Override
    public int getInteger(int columnIndex) throws IOException {
        return (Integer) currentTuple[columnIndex];
    }
    @Override
    public long getBigint(int columnIndex) throws IOException {
        return (Long) currentTuple[columnIndex];
    }
    @Override
    public float getFloat(int columnIndex) throws IOException {
        return (Float) currentTuple[columnIndex];
    }
    @Override
    public double getDouble(int columnIndex) throws IOException {
        return (Double) currentTuple[columnIndex];
    }
    @Override
    public String getVarchar(int columnIndex) throws IOException {
        return (String) currentTuple[columnIndex];
    }
    @Override
    public ByteArray getVarbin(int columnIndex) throws IOException {
        return (ByteArray) currentTuple[columnIndex];
    }
    @Override
    public Date getDate(int columnIndex) throws IOException {
        return new Date((Long) currentTuple[columnIndex]);
    }
    @Override
    public Time getTime(int columnIndex) throws IOException {
        return new Time((Long) currentTuple[columnIndex]);
    }
    @Override
    public Timestamp getTimestamp(int columnIndex) throws IOException {
        return new Timestamp((Long) currentTuple[columnIndex]);
    }
}
