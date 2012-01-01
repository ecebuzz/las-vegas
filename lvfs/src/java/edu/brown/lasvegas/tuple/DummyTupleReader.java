package edu.brown.lasvegas.tuple;

import java.io.IOException;

import edu.brown.lasvegas.ColumnType;

public class DummyTupleReader extends DefaultTupleReader {
    private final Object[][] data;
    private final int dataCount;
    private int current = 0;
    public DummyTupleReader (ColumnType[] columnTypes, Object[][] data, int dataCount) {
        super (columnTypes);
        this.data = data;
        this.dataCount = dataCount;
    }
    @Override
    public void close() throws IOException {
    }
    @Override
    public String getCurrentTupleAsString() {
        return null;
    }
    @Override
    public boolean next() throws IOException {
        if (current < dataCount) {
            System.arraycopy(data[current], 0, currentData, 0, columnCount);
            ++current;
            return true;
        }
        return false;
    }
}
