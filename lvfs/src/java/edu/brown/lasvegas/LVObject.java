package edu.brown.lasvegas;

import org.apache.hadoop.io.Writable;

/**
 * All metadata objects in this package implement this.
 */
public interface LVObject extends Writable {
    /**
     * Returns the unique identifier of the object.
     * The ID is only unique among the same type of object. 
     */
    int getPrimaryKey ();
    
    /**
     * Returns the type of the object defined in {@link LVObjectType}.
     */
    LVObjectType getObjectType ();
}
