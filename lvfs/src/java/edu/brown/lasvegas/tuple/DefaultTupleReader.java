package edu.brown.lasvegas.tuple;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Default implementation of a few methods in {@link TupleReader}.
 * This class is supposed to be used by a non-buffered, sequential,
 * text-file input reader. For buffered tuple readers backed by
 * columnar files will have its own implementation.
 */
public abstract class DefaultTupleReader implements TupleReader {
    @Override
    public int nextBatch(TupleBuffer buffer) throws IOException {
        // simply iterate
        int count = 0;
        while (!buffer.isFull()) {
            boolean appended = buffer.appendTuple(this);
            if (!appended) {
                break;
            }
        }
        return count;
    }
    @Override
    public boolean getBoolean(int columnIndex) throws IOException {
        return getTinyint(columnIndex) != 0;
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
