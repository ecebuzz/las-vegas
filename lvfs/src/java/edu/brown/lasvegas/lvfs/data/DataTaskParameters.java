package edu.brown.lasvegas.lvfs.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskParameters;

/**
 * Base class for parameter types used to serialize/deserialize
 * task-type-dependent parameters.
 */
public abstract class DataTaskParameters implements TaskParameters {
    /** empty constructor. */
    public DataTaskParameters () {}
    /** de-serialize a byte array. */
    public DataTaskParameters (byte[] serializedParameters) throws IOException {
        readFromBytes (serializedParameters);
    }
    /** de-serialize the parameter byte array in LVTask. */
    public DataTaskParameters (LVTask task) throws IOException {
        if (task.getParameters() == null) {
            return;
        }
        DataInput in = new DataInputStream(new ByteArrayInputStream(task.getParameters()));
        readFields (in);
    }

    @Override
    public final void readFromBytes (byte[] serializedParameters) throws IOException {
        DataInput in = new DataInputStream(new ByteArrayInputStream(serializedParameters));
        readFields (in);
    }

    @Override
    public final byte[] writeToBytes () throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(bytes);
        write(out);
        return bytes.toByteArray();
    }
}
