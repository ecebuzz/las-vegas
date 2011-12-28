package edu.brown.lasvegas.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.sleepycat.persist.model.Persistent;

import edu.brown.lasvegas.ColumnType;

/**
 * Pair of beginning (inclusive) and ending (exclusive) keys.
 * Used to represent some value range.
 */
@Persistent
public class ValueRange implements Writable {
    /** type of the value. */
    private ColumnType type;

    /**
     * The starting key of the range (inclusive).
     */
    private Comparable<?> startKey;

    /**
     * The ending key of the range (exclusive).
     */
    private Comparable<?> endKey;
    
    public ValueRange () {}
    public ValueRange (ColumnType type, Comparable<?> startKey, Comparable<?> endKey) {
        this.type = type;
        this.startKey = startKey;
        this.endKey = endKey;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ValueRange)) {
            return false;
        }
        ValueRange o = (ValueRange) obj;
        if (o.type != type) {
            return false;
        }
        if (startKey == null || o.startKey == null) {
            if (startKey != null || o.startKey != null) {
                return false;
            }
        } else {
            if (!startKey.equals(o.startKey)) {
                return false;
            }
        }

        if (endKey == null || o.endKey == null) {
            return (endKey == null && o.endKey == null);
        } else {
            return endKey.equals(o.endKey);
        }
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "[type=" + type + ", " + startKey + "-" + endKey + "]";
    }

    /**
     * Gets the starting key of the range (inclusive).
     *
     * @return the starting key of the range (inclusive)
     */
    public Comparable<?> getStartKey() {
        return startKey;
    }

    /**
     * Sets the starting key of the range (inclusive).
     *
     * @param startKey the new starting key of the range (inclusive)
     */
    public void setStartKey(Comparable<?> startKey) {
        this.startKey = startKey;
    }

    /**
     * Gets the ending key of the range (exclusive).
     *
     * @return the ending key of the range (exclusive)
     */
    public Comparable<?> getEndKey() {
        return endKey;
    }

    /**
     * Sets the ending key of the range (exclusive).
     *
     * @param endKey the new ending key of the range (exclusive)
     */
    public void setEndKey(Comparable<?> endKey) {
        this.endKey = endKey;
    }
    
    /**
     * Gets the type of the value.
     *
     * @return the type of the value
     */
    public ColumnType getType() {
        return type;
    }
    
    /**
     * Sets the type of the value.
     *
     * @param type the new type of the value
     */
    public void setType(ColumnType type) {
        this.type = type;
    }

    /**
     * Returns if the given key falls into this range.
     */
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> boolean contains (T key) {
        // if startKey is null, no lower-bound
        if (startKey != null) {
            if (key == null) {
                return false;
            }
            if (key.compareTo((T) startKey) < 0) {
                return false;
            }
        }
        if (endKey != null) {
            if (key != null) {
                if (key.compareTo((T) endKey) >= 0) { // endKey is exclusive, so >=
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type == null ? ColumnType.INVALID.ordinal() : type.ordinal());
        if (type != null && type != ColumnType.INVALID) {
            writeValue(out, startKey);
            writeValue(out, endKey);
        }
    }
    private void writeValue(DataOutput out, Comparable<?> val) throws IOException {
        out.writeBoolean(val == null);
        if (val == null) {
            return;
        }
        switch (type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT: out.writeLong((Long) val); break;
        case DOUBLE: out.writeDouble((Double) val); break;
        case FLOAT: out.writeFloat((Float) val); break;
        case INTEGER: out.writeInt((Integer) val); break;
        case SMALLINT: out.writeShort((Short) val); break;
        case BOOLEAN:
        case TINYINT: out.writeByte((Byte) val); break;
        case VARCHAR: out.writeUTF((String) val); break;
        default: throw new IllegalArgumentException("Cannot serialize this type:" + type);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = ColumnType.values()[in.readInt()];
        if (type != ColumnType.INVALID) {
            startKey = readValue(in);
            endKey = readValue(in);
        }
    }
    public static ValueRange read (DataInput in) throws IOException {
        ValueRange obj = new ValueRange();
        obj.readFields(in);
        return obj;
    }

    private Comparable<?> readValue(DataInput in) throws IOException {
        if (in.readBoolean()) {
            return null;
        }
        switch (type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT: return Long.valueOf(in.readLong());
        case DOUBLE: return Double.valueOf(in.readDouble());
        case FLOAT: return Float.valueOf(in.readFloat());
        case INTEGER: return Integer.valueOf(in.readInt());
        case SMALLINT: return Short.valueOf(in.readShort());
        case BOOLEAN:
        case TINYINT: return Byte.valueOf(in.readByte());
        case VARCHAR: return in.readUTF();
        default: throw new IllegalArgumentException("Cannot deserialize this type:" + type);
        }
    }
}
