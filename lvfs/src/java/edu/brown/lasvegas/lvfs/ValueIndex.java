package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.LVReplicaScheme;

/**
 * A value index is a sparse index file for sorting columns ({@link LVReplicaScheme#getSortColumnId()}).
 * 
 * <p>A value index file is always paired with a position index file ({@link PositionIndex}).
 * A value index file stores just an array of values corresponding to each entry in the position index file.
 * In other words, you need to use value indexes in two steps; get the position in index file by value index,
 * then get the tuple/byte position in data file by position index.
 * 
 * <p>Just like position index, this file is supposed to be small. So, we read/write them at once.</p>
 */
public interface ValueIndex<T> {
    /**
     * Returns the entry in the corresponding position index file.
     * The returned value should be then passed to {@link PositionIndex#getEntry(int)}.
     * @param value the value to find. -1 if the data file has no chance to contain the key.
     * @return the entry in the corresponding position index file.
     */
    public int searchValues (T value);

    /**
     * Writes out the value index file.
     */
    public void writeToFile (VirtualFile file) throws IOException;
}
