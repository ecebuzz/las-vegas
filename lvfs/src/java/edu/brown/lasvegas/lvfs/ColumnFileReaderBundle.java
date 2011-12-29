package edu.brown.lasvegas.lvfs;

/**
 * Readers to read a set of files which logically constitute a column.
 */
public class ColumnFileReaderBundle {
    private TypedReader<?, ?> dataReader;
    private OrderedDictionary<?, ?> dictionary;
    private PositionIndex positionFile;
    private ValueIndex<?> valueFile;
    
    private final VirtualFile inputFolder;
    /** filename without extension (e.g., "1_2_3" will generate "1_2_3.dat", "1_2_3.pos", and "1_2_3.dic"). */
    private final String filenameSeed;
    
    public ColumnFileReaderBundle (VirtualFile inputFolder, String filenameSeed) {
        this.inputFolder = inputFolder;
        this.filenameSeed = filenameSeed;
    }
}
