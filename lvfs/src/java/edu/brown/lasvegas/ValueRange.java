package edu.brown.lasvegas;

import com.sleepycat.persist.model.Persistent;

/**
 * Pair of beginning (inclusive) and ending (exclusive) keys.
 * Used to represent some value range.
 */
@Persistent
public class ValueRange {
    /**
     * The starting key of the range (inclusive).
     */
    private Comparable<?> startKey;

    /**
     * The ending key of the range (exclusive).
     */
    private Comparable<?> endKey;
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "[" + startKey + "-" + endKey + "]";
    }

    /**
     * Gets the starting key of the range (inclusive).
     *
     * @return the starting key of the range (inclusive)
     */
    public Comparable<?> getStartKey() {
        return startKey;
    }

    /**
     * Sets the starting key of the range (inclusive).
     *
     * @param startKey the new starting key of the range (inclusive)
     */
    public void setStartKey(Comparable<?> startKey) {
        this.startKey = startKey;
    }

    /**
     * Gets the ending key of the range (exclusive).
     *
     * @return the ending key of the range (exclusive)
     */
    public Comparable<?> getEndKey() {
        return endKey;
    }

    /**
     * Sets the ending key of the range (exclusive).
     *
     * @param endKey the new ending key of the range (exclusive)
     */
    public void setEndKey(Comparable<?> endKey) {
        this.endKey = endKey;
    }
}
