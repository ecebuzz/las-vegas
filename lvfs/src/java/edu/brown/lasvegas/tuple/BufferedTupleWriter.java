package edu.brown.lasvegas.tuple;

import java.io.File;
import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.OrderedDictionary;

/**
 * Buffered implementation of TupleWriter.
 * This implementation always outputs the resulting files
 * to the local file system.
 */
public class BufferedTupleWriter implements TupleWriter {
    public BufferedTupleWriter(ColumnType[] columnTypes, CompressionType[] compressionTypes, int bufferSize, File folder, String[] fileNameSeeds) {
        assert (columnTypes.length == compressionTypes.length);
        assert (columnTypes.length == fileNameSeeds.length);
        this.columnTypes = columnTypes;
        this.compressionTypes = compressionTypes;
        this.columnCount = columnTypes.length;
        this.bufferSize = bufferSize;
        this.folder = folder;
        this.fileNameSeeds = fileNameSeeds;
        columnTypesBeforeDecompression = new ColumnType[columnCount];
    }
    private final int bufferSize;
    private final int columnCount;
    private final ColumnType[] columnTypes;
    private final CompressionType[] compressionTypes;
    private final File folder;
    /** filename without extension (e.g., "1_2_3" will generate "1_2_3.dat", "1_2_3.pos", and "1_2_3.dic"). */
    private final String[] fileNameSeeds;
    private TupleBuffer buffer;
    
    /** consider VARCHAR as byte/short/int if it's dictionary-compressed. */
    private final ColumnType[] columnTypesBeforeDecompression;

    private TupleReader reader;
    private CompressionType[] readerCompressionTypes;
    private OrderedDictionary<?>[] readerDictionaries;
    
    private int tuplesWritten = 0;
    
    @Override
    public void init(TupleReader reader) throws IOException {
        this.reader = reader;
        if (reader.getColumnCount() != columnCount) {
            throw new IOException ("column count doesn't match. writer=" + columnCount + ", reader=" + reader.getColumnCount());
        }
        for (int i = 0; i < columnCount; ++i) {
            if (reader.getColumnType(i) != columnTypes[i]) {
                throw new IOException ("column type[" + i + "] doesn't match. writer=" + columnTypes[i] + ", reader=" + reader.getColumnType(i));
            }
        }
        readerCompressionTypes = reader.getCompressionTypes().clone();
        readerDictionaries = new OrderedDictionary<?>[columnCount];

        // if the reader (original file) is dictionary compressed, reuse it!
        // then we simply treat the data as integers.
        for (int i = 0; i < columnCount; ++i) {
            if (compressionTypes[i] == CompressionType.DICTIONARY) {
                readerDictionaries[i] = reader.getDictionary(i);
                switch (readerDictionaries[i].getBytesPerEntry()) {
                case 1: columnTypesBeforeDecompression[i] = ColumnType.TINYINT;break;
                case 2: columnTypesBeforeDecompression[i] = ColumnType.SMALLINT;break;
                default:
                    assert (readerDictionaries[i].getBytesPerEntry() == 4);
                    columnTypesBeforeDecompression[i] = ColumnType.INTEGER;
                    break;
                }
                // TODO copy dictionary file
            } else {
                columnTypesBeforeDecompression[i] = columnTypes[i];
            }
        }
        this.buffer = new TupleBuffer(columnTypesBeforeDecompression, bufferSize);
    }

    @Override
    public int appendAllTuples() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void flush(boolean sync) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void writeFileFooter() throws IOException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
    @Override
    public int getTupleCount() throws IOException {
        return tuplesWritten;
    }
}
