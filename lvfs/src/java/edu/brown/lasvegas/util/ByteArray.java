package edu.brown.lasvegas.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * Comparable and writable byte array.
 * Adds a slight overhead compared to raw byte[], but shouldn't be critical.
 */
public final class ByteArray implements Comparable<ByteArray>, Writable, Cloneable {
    private byte[] bytes;
    public ByteArray() {}
    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }
    public ByteArray(ByteArray o) {
        this.bytes = o.bytes == null ? null : o.bytes.clone(); // clone because byte[] is mutable
    }
    public byte[] getBytes () {
        return bytes;
    }
    public void setBytes (byte[] bytes) {
        this.bytes = bytes;
    }
    @Override
    public int compareTo(ByteArray o) {
        if (bytes == null && o.bytes == null) {
            return 0;
        }
        if (bytes == null) {
            return -1;
        }
        if (o.bytes == null) {
            return 1;
        }
        int len = Math.min(bytes.length, o.bytes.length);
        for (int i = 0; i < len; ++i) {
            if (bytes[i] != o.bytes[i]) {
                return bytes[i] - o.bytes[i];
            }
        }
        return bytes.length - o.bytes.length;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof ByteArray)) return false;
        ByteArray other = (ByteArray) obj;
        if (this == other) return true;
        return (Arrays.equals(bytes, other.bytes));
    }
    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
    @Override
    public String toString() {
        return Arrays.toString(bytes);
    }
    @Override
    protected ByteArray clone() throws CloneNotSupportedException {
        return new ByteArray (this);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        assert (len >= -1);
        if (len == -1) {
            bytes = null;
        } else {
            bytes = new byte[len];
            in.readFully(bytes);
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bytes == null ? -1 : bytes.length);
    }
}
