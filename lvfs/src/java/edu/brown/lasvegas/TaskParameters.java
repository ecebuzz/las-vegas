package edu.brown.lasvegas;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Marker interface for parameter types used to serialize/deserialize
 * task-type-dependent parameters.
 */
public interface TaskParameters extends Writable {
    /**
     * De-serialize from the given byte array.
     */
    public void readFromBytes (byte[] serializedParameters) throws IOException;

    /**
     * Serialize this object into a byte array.
     */
    public byte[] writeToBytes () throws IOException;
}
