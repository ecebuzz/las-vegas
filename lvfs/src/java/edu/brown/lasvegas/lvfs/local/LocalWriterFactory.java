package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.traits.FixLenValueTraits;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.VarLenValueTraits;

/**
 * factory class for columnar file writers.
 */
public final class LocalWriterFactory {
    /**
     * Instantiate a file writer for the given file(s) and compression type.
     * @param fileBundle the file to output
     * @param compression the compression to apply while the writes.
     * @param traits the data type traits BEFORE compression.
     * @return writer object
     * @throws IOException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static TypedWriter<?, ?> getInstance(ColumnFileBundle fileBundle, CompressionType compression, ValueTraits<?,?> traits) throws IOException {
        switch (compression) {
        case DICTIONARY:
            return new LocalDictCompressionWriter(fileBundle.getDataFile(), fileBundle.getDictionaryFile(), fileBundle.getTmpFile(), traits);
        case RLE:
            return new LocalRLEWriter(fileBundle.getDataFile(), traits);
        case GZIP_BEST_COMPRESSION:
        case SNAPPY:
            if (traits instanceof VarLenValueTraits<?>) {
                return new LocalBlockCompressionVarLenWriter(fileBundle.getDataFile(), (VarLenValueTraits<?>) traits, compression);
            } else {
                return new LocalBlockCompressionFixLenWriter(fileBundle.getDataFile(), (FixLenValueTraits<?, ?>) traits, compression);
            }
        case NONE:
            if (traits instanceof VarLenValueTraits<?>) {
                return new LocalVarLenWriter(fileBundle.getDataFile(), (VarLenValueTraits<?>) traits);
            } else {
                return new LocalFixLenWriter(fileBundle.getDataFile(), (FixLenValueTraits<?, ?>) traits);
            }
        default:
            throw new IllegalArgumentException("unexpected compression type:" + compression);
        }
    }
    
    private LocalWriterFactory() {}
}
