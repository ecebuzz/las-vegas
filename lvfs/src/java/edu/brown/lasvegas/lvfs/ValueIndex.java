package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.LVReplicaScheme;

/**
 * A value index is a sparse index file for sorting columns ({@link LVReplicaScheme#getSortColumnId()}).
 * 
 * <p>A value index file stores just values and its tuple positions, not the byte positions.
 * To seek to the tuple position, one might need a position index file ({@link PositionIndex}).
 * In other words, you might need to use value indexes in two steps; get the tuple position by value index,
 * then get the byte position in data file by position index. The second step might not needed for a few
 * data file formats. For example, byte position in fixed-len file is a multiply of tuple position,
 * block-compressed data file has a position index in itself.</p>
 * 
 * <p>Just like position index, this file is supposed to be small. So, we read/write them at once.</p>
 */
public interface ValueIndex<T extends Comparable<T>> {
    /**
     * Returns the tuple position to start reading.
     * @param value the value to find.
     * @return the tuple position to start reading. -1 if the data file has no chance to contain the key.
     */
    public int searchValues (T value);

    /**
     * Writes out the value index file.
     */
    public void writeToFile (VirtualFile file) throws IOException;
}
