package edu.brown.lasvegas;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Marker interface for parameter types used to serialize/deserialize
 * task-type-dependent parameters.
 */
public abstract class TaskParameters implements Writable {
    /** empty constructor. */
    public TaskParameters () {}
    /** de-serialize a byte array. */
    public TaskParameters (byte[] serializedParameters) throws IOException {
        DataInput in = new DataInputStream(new ByteArrayInputStream(serializedParameters));
        readFields (in);
    }
    /** de-serialize the parameter byte array in LVTask. */
    public TaskParameters (LVTask task) throws IOException {
        if (task.getParameters() == null) {
            return;
        }
        DataInput in = new DataInputStream(new ByteArrayInputStream(task.getParameters()));
        readFields (in);
    }

    /**
     * Serialize this object into a byte array.
     */
    public final byte[] writeToBytes () throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(bytes);
        write(out);
        return bytes.toByteArray();
    }
}
