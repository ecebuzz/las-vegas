package edu.brown.lasvegas.lvfs;

import java.io.File;

/**
 * Writers to write out a set of files which logically constitute a column.
 * @param <T> Original data type.
 * @param <DT> Entry type in the data file. Same as original data type except it's dictionary compressed, in which case DT is Byte/Short/Integer.
 * @param <DA> Array entry type in the data file.
 */
public class ColumnFileWriterBundle<T extends Comparable<T>, DT extends Comparable<DT>, DA> {
    private TypedWriter<DT, DA> dataFile;
    private OrderedDictionary<T, T[]> dictionaryFile;
    private PositionIndex positionFile;
    private ValueIndex<DT> valueFile;
    
    private File outputFolder;
    private String fileNameSeed;

    public static <T extends Comparable<T>, DT extends Comparable<DT>, DA>
        ColumnFileWriterBundle<T, DT, DA> createLocalWriterBundle (File outputFolder, String fileNameSeed) {
        ColumnFileWriterBundle<T, DT, DA> bundle = new ColumnFileWriterBundle<T, DT, DA>();
        return bundle;
    }
    /*
    public ColumnFileWriterBundle (File outputFolder, String fileNameSeed) {
        this.outputFolder = outputFolder;
        this.fileNameSeed = fileNameSeed;
    }
    */
}
