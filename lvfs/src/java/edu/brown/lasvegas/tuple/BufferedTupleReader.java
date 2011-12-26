package edu.brown.lasvegas.tuple;

import java.io.IOException;
import java.io.InputStream;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.TypedReader;

public abstract class BufferedTupleReader implements TupleReader {
    public BufferedTupleReader (ColumnType[] columnTypes, CompressionType[] compressionTypes, int bufferSize, InputStream[] streams) throws IOException {
        this.columnTypes = columnTypes;
        this.compressionTypes = compressionTypes;
        this.buffer = new TupleBuffer (columnTypes, bufferSize);
        this.streams = streams;
    }
    private final ColumnType[] columnTypes;
    private final CompressionType[] compressionTypes;
    private final TupleBuffer buffer;
    private TypedReader<?, ?> columnReaders;
    private InputStream[] streams;
}
