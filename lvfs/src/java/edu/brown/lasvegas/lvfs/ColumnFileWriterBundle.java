package edu.brown.lasvegas.lvfs;

/**
 * Triplet of columnar-file writers which logically constitutes a column writer.
 * @param <T> Original data type.
 * @param <DT> Entry type in the data file. Same as original data type except it's dictionary compressed, in which case DT is Byte/Short/Integer.
 * @param <DA> Array entry type in the data file.
 */
public class ColumnFileWriterBundle<T extends Comparable<T>, DT extends Comparable<DT>, DA> {
    private TypedWriter<DT, DA> dataWriter;
    private OrderedDictionary<T> dictionary;
    private PositionIndex positionFile;
}
