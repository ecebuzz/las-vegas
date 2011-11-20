package edu.brown.lasvegas.util;

import com.sleepycat.persist.model.Persistent;

/**
 * Pair of beginning (inclusive) and ending (exclusive) keys.
 * Used to represent some value range.
 */
@Persistent
public class ValueRange<T extends Comparable<T>> {
    /**
     * The starting key of the range (inclusive).
     */
    private T startKey;

    /**
     * The ending key of the range (exclusive).
     */
    private T endKey;
    
    public ValueRange () {}
    public ValueRange (T startKey, T endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ValueRange)) {
            return false;
        }
        ValueRange<?> o = (ValueRange<?>) obj;
        return startKey.equals(o.startKey) && endKey.equals(o.endKey);
    }
    
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
    public T getStartKey() {
        return startKey;
    }

    /**
     * Sets the starting key of the range (inclusive).
     *
     * @param startKey the new starting key of the range (inclusive)
     */
    public void setStartKey(T startKey) {
        this.startKey = startKey;
    }

    /**
     * Gets the ending key of the range (exclusive).
     *
     * @return the ending key of the range (exclusive)
     */
    public T getEndKey() {
        return endKey;
    }

    /**
     * Sets the ending key of the range (exclusive).
     *
     * @param endKey the new ending key of the range (exclusive)
     */
    public void setEndKey(T endKey) {
        this.endKey = endKey;
    }

    /**
     * Returns if the given key falls into this range.
     */
    public boolean contains (T key) {
        return startKey.compareTo(key) >= 0 && endKey.compareTo(key) < 0;
    }
}
