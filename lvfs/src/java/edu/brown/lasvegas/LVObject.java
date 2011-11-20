package edu.brown.lasvegas;

/**
 * All metadata objects in this package implement this.
 */
public interface LVObject {
    /**
     * Returns the unique identifier of the object.
     * The ID is only unique among the same type of object. 
     */
    int getPrimaryKey ();
}
