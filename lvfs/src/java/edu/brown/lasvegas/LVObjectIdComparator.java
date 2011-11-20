package edu.brown.lasvegas;

import java.util.Comparator;

/**
 * ID Comparator for LVObject.
 */
public class LVObjectIdComparator<T extends LVObject> implements Comparator<T> {
    public int compare(T o1, T o2) {
        return o1.getPrimaryKey() - o2.getPrimaryKey();
    }
}
