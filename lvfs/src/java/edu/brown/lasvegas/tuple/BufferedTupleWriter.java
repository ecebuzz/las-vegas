package edu.brown.lasvegas.tuple;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileWriterBundle;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Buffered implementation of TupleWriter.
 * This implementation always outputs the resulting files
 * to the local file system.
 */
public class BufferedTupleWriter implements TupleWriter {
    private static Logger LOG = Logger.getLogger(BufferedTupleWriter.class);

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

    public BufferedTupleWriter(TupleReader reader, int bufferSize, VirtualFile outputFolder, CompressionType[] compressionTypes, String[] fileNameSeeds, boolean calculateChecksum) throws IOException {
        this (reader, bufferSize, outputFolder, compressionTypes, fileNameSeeds, calculateChecksum, null);
    }
    public BufferedTupleWriter(
            TupleReader reader,
            int bufferSize,
            VirtualFile outputFolder,
            CompressionType[] compressionTypes,
            String[] fileNameSeeds,
            boolean calculateChecksum,
            BatchCallback callback) throws IOException {
        this.columnTypes = reader.getColumnTypes();
        this.compressionTypes = compressionTypes;
        this.columnCount = columnTypes.length;
        this.buffer = new TupleBuffer(columnTypes, bufferSize);
        this.reader = reader;
        this.callback = callback;
        this.columnWriters = new ColumnFileWriterBundle[columnCount];

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
        for (int i = 0; i < columnCount; ++i) {
            columnWriters[i] = new ColumnFileWriterBundle(outputFolder, fileNameSeeds[i], columnTypes[i], compressionTypes[i], calculateChecksum);
        }
    }
    
    private final int columnCount;
    private final ColumnType[] columnTypes;
    private final CompressionType[] compressionTypes;
    private final TupleBuffer buffer;
    private final TupleReader reader;
    private final BatchCallback callback;
    
    private ColumnFileWriterBundle[] columnWriters;
    
    private int tuplesWritten = 0;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int appendAllTuples() throws IOException {
        LOG.info("receiving/writing all tuples...");
        while (true) {
            buffer.resetCount();
            int read = reader.nextBatch(buffer);
            if (read < 0) {
                break;
            }
            LOG.info("read " + read + " tuples.");
            for (int i = 0; i < columnCount; ++i) {
                Object columnData = buffer.getColumnBuffer(i);
                ((TypedWriter) columnWriters[i].getDataWriter()).writeValues(columnData, 0, read);
            }
            LOG.info("wrote " + read + " tuples to " + columnCount + " column files.");
            tuplesWritten += read;
            if (callback != null) {
                boolean continued = callback.onBatchWritten(tuplesWritten);
                if (!continued) {
                    LOG.warn("callback function requested to terminate. exitting..");
                    break;
                }
            }
        }
        LOG.info("done.");
        return tuplesWritten;
    }

    @Override
    public ColumnFileBundle[] finish() throws IOException {
        ColumnFileBundle[] fileBundles = new ColumnFileBundle[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            ColumnFileWriterBundle writer = columnWriters[i];
            writer.finish();
            ColumnFileBundle bundle = new ColumnFileBundle();
            bundle.setColumnType(columnTypes[i]);
            bundle.setCompressionType(compressionTypes[i]);
            bundle.setDataFile(writer.getDataFile());
            bundle.setDataFileChecksum(writer.getDataFileChecksum());
            bundle.setDictionaryBytesPerEntry(writer.getDictionaryBytesPerEntry());
            bundle.setDictionaryFile(writer.getDictionaryFile());
            bundle.setDistinctValues(writer.getDistinctValues());
            bundle.setPositionFile(writer.getPositionFile());
            bundle.setRunCount(writer.getRunCount());
            bundle.setSorted(false);
            bundle.setTupleCount(tuplesWritten);
            bundle.setUncompressedSizeKB(writer.getUncompressedSizeKB());
            // bundle.setValueFile(writer.getValueFile()); because BufferedTupleWriter sequentially writes, it never outputs a value index file
            fileBundles[i] = bundle;
        }
        return fileBundles;
    }
    
    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnCount; ++i) {
            columnWriters[i].getDataWriter().close();
        }
    }
    @Override
    public int getTupleCount() throws IOException {
        return tuplesWritten;
    }
    @Override
    public int getColumnCount() {
        return columnCount;
    }
}
