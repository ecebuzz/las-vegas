package edu.brown.lasvegas.tuple;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.util.ByteArray;

/**
 * Default implementation of a few methods in {@link TupleReader}.
 * This class is supposed to be used by a non-buffered, sequential,
 * text-file input reader. For buffered tuple readers backed by
 * columnar files will have its own implementation.
 */
public abstract class DefaultTupleReader implements TupleReader {
    public DefaultTupleReader (ColumnType[] columnTypes) {
        this.columnCount = columnTypes.length;
        this.columnTypes = columnTypes;
        this.currentData = new Object[columnCount];
    }
    protected final int columnCount;
    protected final ColumnType[] columnTypes;
    /** current tuple data after parsing. #next() must set values to this array that agree with #columnTypes. */
    protected final Object[] currentData;

    @Override
    public int nextBatch(TupleBuffer buffer) throws IOException {
        // simply iterate
        int count = 0;
        while (!buffer.isFull()) {
            boolean appended = buffer.appendTuple(this);
            if (!appended) {
                if (count > 0) {
                    return count;
                } else {
                    return -1; // end of file
                }
            }
            ++count;
        }
        return count;
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
        return currentData[columnIndex];
    }

    @Override
    public boolean getBoolean(int columnIndex) throws IOException {
        return getTinyint(columnIndex) != 0;
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

    @Override
    public Date getDate(int columnIndex) throws IOException {
        return new Date(getBigint(columnIndex));
    }
    @Override
    public Time getTime(int columnIndex) throws IOException {
        return new Time(getBigint(columnIndex));
    }
    @Override
    public Timestamp getTimestamp(int columnIndex) throws IOException {
        return new Timestamp(getBigint(columnIndex));
    }
}
