package edu.brown.lasvegas;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

/** composite class to create a composite secondary index in BDB-JE. */
@Persistent
public class CompositeIntKey implements Comparable<CompositeIntKey> {
    /** empty constructor. */
    public CompositeIntKey() {
        
    }
    /** constructor with values. */
    public CompositeIntKey(int value1, int value2) {
        this.value1 = value1;
        this.value2 = value2;
    }
    
    /** The first value. */
    @KeyField(1)
    private int value1;
    
    /** The second value. */
    @KeyField(2)
    private int value2;
    
    @Override
    public int compareTo(CompositeIntKey o) {
        if (value1 != o.value1) return value1 - o.value1;
        return value2 - o.value2;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CompositeIntKey) {
            CompositeIntKey o = (CompositeIntKey) obj;
            return value1 == o.value1 && value2 == o.value2;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return value1 * 0xD143B5A7 + value2;
    }

    @Override
    public String toString() {
        return value1 + "," + value2;
    }
    
    /**
     * Gets the first value.
     *
     * @return the first value
     */
    public int getValue1() {
        return value1;
    }
    
    /**
     * Sets the first value.
     *
     * @param value1 the new first value
     */
    public void setValue1(int value1) {
        this.value1 = value1;
    }
    
    /**
     * Gets the second value.
     *
     * @return the second value
     */
    public int getValue2() {
        return value2;
    }
    
    /**
     * Sets the second value.
     *
     * @param value2 the new second value
     */
    public void setValue2(int value2) {
        this.value2 = value2;
    }
}
