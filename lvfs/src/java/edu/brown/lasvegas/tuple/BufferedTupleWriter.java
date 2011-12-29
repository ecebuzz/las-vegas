package edu.brown.lasvegas.tuple;

import java.io.File;
import java.io.IOException;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileWriterBundle;

/**
 * Buffered implementation of TupleWriter.
 * This implementation always outputs the resulting files
 * to the local file system.
 */
public class BufferedTupleWriter implements TupleWriter {
    /**
     * Callback interface for {@link TupleWriter#appendAllTuples()} to do something after each batch write.
     */
    public static interface BatchCallback {
        /**
         * Called after each batch write during {@link TupleWriter#appendAllTuples()}.
         * @param totalTuplesWritten total number of tuples written to files so far.
         * @return true to continue. false to terminate the writer.
         */
        boolean onBatchWritten (int totalTuplesWritten) throws IOException;
    }

    public BufferedTupleWriter(TupleReader reader, int bufferSize, File outputFolder, CompressionType[] compressionTypes, String[] fileNameSeeds) throws IOException {
        this (reader, bufferSize, outputFolder, compressionTypes, fileNameSeeds, null);
    }
    public BufferedTupleWriter(
            TupleReader reader,
            int bufferSize,
            File outputFolder,
            CompressionType[] compressionTypes,
            String[] fileNameSeeds,
            BatchCallback callback) throws IOException {
        this.columnTypes = reader.getColumnTypes();
        this.compressionTypes = compressionTypes;
        this.columnCount = columnTypes.length;
        this.outputFolder = outputFolder;
        this.fileNameSeeds = fileNameSeeds;
        this.buffer = new TupleBuffer(columnTypes, bufferSize);
        this.reader = reader;
        this.callback = callback;
        this.columnWriters = new ColumnFileWriterBundle<?, ?, ?>[columnCount];

        // check parameters
        if (columnTypes.length != compressionTypes.length) {
            throw new IllegalArgumentException("length of columnTypes/compressionTypes doesn't match");
        }
        if (columnTypes.length != fileNameSeeds.length) {
            throw new IllegalArgumentException("length of columnTypes/fileNameSeeds doesn't match");
        }
        if (reader.getColumnCount() != columnCount) {
            throw new IllegalArgumentException ("column count doesn't match. writer=" + columnCount + ", reader=" + reader.getColumnCount());
        }
        for (int i = 0; i < columnCount; ++i) {
            if (reader.getColumnType(i) != columnTypes[i]) {
                throw new IllegalArgumentException ("column type[" + i + "] doesn't match. writer=" + columnTypes[i] + ", reader=" + reader.getColumnType(i));
            }
            // although column types must match between reader/writer, compression types are often different.
        }
    }
    private void initColumnWriters () throws IOException {
        for (int i = 0; i < columnCount; ++i) {
            
        }
    }
    private final int columnCount;
    private final ColumnType[] columnTypes;
    private final CompressionType[] compressionTypes;
    /** folder to output all columnar files. */
    private final File outputFolder;
    /** filename without extension (e.g., "1_2_3" will generate "1_2_3.dat", "1_2_3.pos", and "1_2_3.dic"). */
    private final String[] fileNameSeeds;
    private final TupleBuffer buffer;
    private final TupleReader reader;
    private final BatchCallback callback;
    
    private ColumnFileWriterBundle<?, ?, ?>[] columnWriters;
    
    private int tuplesWritten = 0;

    @Override
    public int appendAllTuples() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void finish() throws IOException {
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
    @Override
    public int getColumnCount() {
        return columnCount;
    }
    
    @Override
    public ColumnFileWriterBundle<?, ?, ?> getColumnWriterBundle(int col) {
        return columnWriters[col];
    }
}
