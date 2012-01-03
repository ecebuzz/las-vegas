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
 * job-type-dependent parameters.
 */
public abstract class JobParameters implements Writable {
    /** empty constructor. */
    public JobParameters () {}
    /** de-serialize a byte array. */
    public JobParameters (byte[] serializedParameters) throws IOException {
        DataInput in = new DataInputStream(new ByteArrayInputStream(serializedParameters));
        readFields (in);
    }

    /** de-serialize the parameter byte array in LVJob. */
    public JobParameters (LVJob job) throws IOException {
        if (job.getParameters() == null) {
            return;
        }
        DataInput in = new DataInputStream(new ByteArrayInputStream(job.getParameters()));
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
    
    protected String readNillableString (DataInput in) throws IOException {
        boolean isNull = in.readBoolean();
        if (isNull) {
            return null;
        } else {
            return in.readUTF();
        }
    }

    protected void writeNillableString (DataOutput out, String string) throws IOException {
        out.writeBoolean (string == null);
        if (string != null) {
            out.writeUTF(string);
        }
    }
}
