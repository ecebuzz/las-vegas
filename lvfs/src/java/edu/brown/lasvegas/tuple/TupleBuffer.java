package edu.brown.lasvegas.tuple;

import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.TypedReader;

/**
 * A fixed-size buffer class to tentatively store a number of tuples in
 * type-specific arrays.
 * <p>Rather than storing everything in Object[], this buffer class
 * allows optimization exploiting primitive types.</p>
 */
public class TupleBuffer {
    /**
     * Constructs a fixed-size (so, no automatic extension) buffer.
     * @param types value type of each column 
     * @param bufferSize maximum number of tuples to buffer.
     */
    public TupleBuffer (ColumnType[] types, int bufferSize) {
        this.types = types;
        this.bufferSize = bufferSize;
        this.columnCount = types.length;
        this.data = new Object[columnCount];
        this.accessors = new ColumnAccessor[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            if (this.types[i] == null) {
                continue;
            }
            switch (this.types[i]) {
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                data[i] = new long[bufferSize];
                accessors[i] = new LongColumnAccessor(i);
                break;
            case INTEGER:
                data[i] = new int[bufferSize];
                accessors[i] = new IntColumnAccessor(i);
                break;
            case SMALLINT:
                data[i] = new short[bufferSize];
                accessors[i] = new ShortColumnAccessor(i);
                break;
            case BOOLEAN:
            case TINYINT:
                data[i] = new byte[bufferSize];
                accessors[i] = new ByteColumnAccessor(i);
                break;
            case FLOAT:
                data[i] = new float[bufferSize];
                accessors[i] = new FloatColumnAccessor(i);
                break;
            case DOUBLE:
                data[i] = new double[bufferSize];
                accessors[i] = new DoubleColumnAccessor(i);
                break;
            case VARCHAR:
                data[i] = new String[bufferSize];
                accessors[i] = new StringColumnAccessor(i);
                break;
            case VARBINARY:
                data[i] = new byte[bufferSize][];
                accessors[i] = new ByteArrayColumnAccessor(i);
                break;
            default:
                throw new RuntimeException ("unexpected column type:" + types[i]); 
            }
        }
    }
    
    /** returns the number of tuples currently buffered. */
    public int getCount ()  {
        return count;
    }
    /** Sets zero to count. In other words, makes this buffer empty for next use. */
    public void resetCount () {
        count = 0;
    }
    /** returns the maximum number of tuples this buffer can hold. */
    public int getBufferSize()  {
        return bufferSize;
    }
    /** returns the number of columns this buffer assume. */
    public int getColumnCount()  {
        return columnCount;
    }
    
    public Object getColumnBuffer (int col) {
        return data[col];
    }
    public long[] getColumnBufferAsLong (int col) {
        return (long[]) data[col];
    }
    public int[] getColumnBufferAsInt (int col) {
        return (int[]) data[col];
    }
    public short[] getColumnBufferAsShort (int col) {
        return (short[]) data[col];
    }
    public byte[] getColumnBufferAsByte (int col) {
        return (byte[]) data[col];
    }
    public float[] getColumnBufferAsFloat (int col) {
        return (float[]) data[col];
    }
    public double[] getColumnBufferAsDouble (int col) {
        return (double[]) data[col];
    }
    public String[] getColumnBufferAsString (int col) {
        return (String[]) data[col];
    }
    
    /**
     * Append a tuple to this buffer from the given table reader.
     * Mainly used while data import.
     * @return whether a tuple is appended. false if the given reader
     * had no tuple to provide or the buffer was full. 
     */
    public boolean appendTuple (TupleReader reader)  throws IOException {
        if (count >= bufferSize) {
            return false;
        }
        if (!reader.next()) {
            return false;
        }
        for (int i = 0; i < columnCount; ++i) {
            if (accessors[i] != null) {
                accessors[i].put (reader);
            }
        }
        ++count;
        return true;
    }
    
    /**
     * Append as many tuples as possible to this buffer from the column file readers.
     * @param columnReaders column file readers
     * @return the number of tuples read from the column files and appended to this buffer 
     * @throws IOException
     */
    public int appendTuples (TypedReader<?,?>[] columnReaders) throws IOException {
        return appendTuples(bufferSize - count, columnReaders);
    }

    /**
     * Append (at most) the specified number of tuples to this buffer from the column file readers.
     * @param tuplesToRead the number of tuples to read and append.
     * @param columnReaders column file readers
     * @return the number of tuples read from the column files and appended to this buffer 
     * @throws IOException
     */
    public int appendTuples (int tuplesToRead, TypedReader<?,?>[] columnReaders) throws IOException {
        if (tuplesToRead > bufferSize - count) {
            tuplesToRead = bufferSize - count;
        }
        int actuallyRead = -1;
        for (int i = 0; i < columnCount; ++i) {
            if (accessors[i] != null) {
                int read = accessors[i].put(tuplesToRead, columnReaders[i]);
                if (actuallyRead == -1) {
                    actuallyRead = read; 
                } else {
                    assert (actuallyRead == read);
                }
            }
        }
        return actuallyRead;
    }

    /** the number of tuples currently buffered. */
    private int count = 0;
    
    /** the maximum number of tuples this buffer can hold. */
    private final int bufferSize;
    
    /** buffered data of all columns. i-th column's data = data[i], which might be int[], long[], String[] etc. */
    private final Object[] data;
    
    /** value type of each column. */
    private final ColumnType[] types;
    
    private final int columnCount;
    
    private final ColumnAccessor[] accessors;
    
    private interface ColumnAccessor {
        public void put (TupleReader reader) throws IOException;
        public int put (int tuplesToRead, TypedReader<?,?> reader) throws IOException;
    }
    
    private class LongColumnAccessor implements ColumnAccessor {
        private LongColumnAccessor (int col) {
            this.col = col;
            this.array = (long[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getBigint(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, long[]> typedReader = (TypedReader<?, long[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        long[] array;
        int col;
    }

    private class IntColumnAccessor implements ColumnAccessor {
        private IntColumnAccessor (int col) {
            this.col = col;
            this.array = (int[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getInteger(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, int[]> typedReader = (TypedReader<?, int[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        int[] array;
        int col;
    }

    private class ShortColumnAccessor implements ColumnAccessor {
        private ShortColumnAccessor (int col) {
            this.col = col;
            this.array = (short[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getSmallint(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, short[]> typedReader = (TypedReader<?, short[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        short[] array;
        int col;
    }

    private class ByteColumnAccessor implements ColumnAccessor {
        private ByteColumnAccessor (int col) {
            this.col = col;
            this.array = (byte[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getTinyint(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, byte[]> typedReader = (TypedReader<?, byte[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        byte[] array;
        int col;
    }

    private class FloatColumnAccessor implements ColumnAccessor {
        private FloatColumnAccessor (int col) {
            this.col = col;
            this.array = (float[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getFloat(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, float[]> typedReader = (TypedReader<?, float[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        float[] array;
        int col;
    }


    private class DoubleColumnAccessor implements ColumnAccessor {
        private DoubleColumnAccessor (int col) {
            this.col = col;
            this.array = (double[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getDouble(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, double[]> typedReader = (TypedReader<?, double[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        double[] array;
        int col;
    }

    private class StringColumnAccessor implements ColumnAccessor {
        private StringColumnAccessor (int col) {
            this.col = col;
            this.array = (String[]) data[col];
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getVarchar(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, String[]> typedReader = (TypedReader<?, String[]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        String[] array;
        int col;
    }


    private class ByteArrayColumnAccessor implements ColumnAccessor {
        private ByteArrayColumnAccessor (int col) {
            this.array = (byte[][]) data[col];
            this.col = col;
        }
        @Override
        public void put(TupleReader reader) throws IOException {
            array[count] = reader.getVarbin(col);
        }
        @Override
        public int put(int tuplesToRead, TypedReader<?, ?> reader) throws IOException {
            @SuppressWarnings("unchecked")
            TypedReader<?, byte[][]> typedReader = (TypedReader<?, byte[][]>) reader;
            return typedReader.readValues(array, count, tuplesToRead);
        }
        byte[][] array;
        int col;
    }
}
