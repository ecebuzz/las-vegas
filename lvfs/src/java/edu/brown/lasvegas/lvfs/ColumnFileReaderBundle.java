package edu.brown.lasvegas.lvfs;

/**
 * Triplet of columnar-files which logically constitutes a column (see {@link LVFSFileType}).
 * @param <T> Original data type.
 * @param <DT> Entry type in the data file. Same as original data type except it's dictionary compressed, in which case DT is Byte/Short/Integer.
 * @param <DA> Array entry type in the data file.
 */
public class ColumnFileReaderBundle<T extends Comparable<T>, DT extends Comparable<DT>, DA> {
    private TypedReader<DT, DA> dataReader;
    private OrderedDictionary<T> dictionary;
    private PositionIndex positionFile;
}
