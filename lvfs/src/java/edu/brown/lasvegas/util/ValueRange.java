package edu.brown.lasvegas.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.persist.model.Persistent;

import edu.brown.lasvegas.ColumnType;

/**
 * Pair of beginning (inclusive) and ending (exclusive) keys.
 * Used to represent some value range.
 */
@Persistent
public class ValueRange<T extends Comparable<T>> {
    /**
     * The starting key of the range (inclusive).
     */
    private T startKey;

    /**
     * The ending key of the range (exclusive).
     */
    private T endKey;
    
    public ValueRange () {}
    public ValueRange (T startKey, T endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ValueRange)) {
            return false;
        }
        ValueRange<?> o = (ValueRange<?>) obj;
        return startKey.equals(o.startKey) && endKey.equals(o.endKey);
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "[" + startKey + "-" + endKey + "]";
    }

    /**
     * Gets the starting key of the range (inclusive).
     *
     * @return the starting key of the range (inclusive)
     */
    public T getStartKey() {
        return startKey;
    }

    /**
     * Sets the starting key of the range (inclusive).
     *
     * @param startKey the new starting key of the range (inclusive)
     */
    public void setStartKey(T startKey) {
        this.startKey = startKey;
    }

    /**
     * Gets the ending key of the range (exclusive).
     *
     * @return the ending key of the range (exclusive)
     */
    public T getEndKey() {
        return endKey;
    }

    /**
     * Sets the ending key of the range (exclusive).
     *
     * @param endKey the new ending key of the range (exclusive)
     */
    public void setEndKey(T endKey) {
        this.endKey = endKey;
    }

    /**
     * Returns if the given key falls into this range.
     */
    public boolean contains (T key) {
        return startKey.compareTo(key) >= 0 && endKey.compareTo(key) < 0;
    }

    
    /**
     * serializes a range object into {@link DataOutput}.
     * To use this and deserialization method, the data type must be
     * one of the types defined in {@link ColumnType}. Also, the caller has
     * to provide the data type as a parameter because the type information is
     * erased from ValueRange at runtime.
     */
    public static void writeRange(DataOutput out, ValueRange<?> range, ColumnType type) throws IOException {
        out.writeBoolean(range == null);
        if (range == null) {
            return;
        }
        out.writeInt(type.ordinal());
        writeRangeWithoutHeader(out, range, type);
    }
    private static void writeRangeWithoutHeader(DataOutput out, ValueRange<?> range, ColumnType type) throws IOException {
        out.writeBoolean(range.startKey == null);
        out.writeBoolean(range.endKey == null);
        for (Object val : new Object[]{range.startKey, range.endKey}) {
            if (val == null) {
                switch (type) {
                case BIGINT: out.writeLong((Long) val); break;
                case BOOLEAN: out.writeBoolean((Boolean) val); break;
                case DOUBLE: out.writeDouble((Double) val); break;
                case FLOAT: out.writeFloat((Float) val); break;
                case INTEGER: out.writeInt((Integer) val); break;
                case SMALLINT: out.writeShort((Short) val); break;
                case TINYINT: out.writeByte((Byte) val); break;
                case VARCHAR: out.writeUTF((String) val); break;
                default: throw new IllegalArgumentException("Cannot serialize this type:" + type);
                }
            }
        }
    }
    /**
     * serializes an array of range objects into {@link DataOutput}.
     */
    public static void writeRanges(DataOutput out, ValueRange<?>[] ranges, ColumnType type) throws IOException {
        out.writeBoolean(ranges == null);
        if (ranges == null) {
            return;
        }
        out.writeInt(type.ordinal());
        final int count = ranges.length;
        out.writeInt(count);
        for (int i = 0; i < count; ++i) {
            out.writeBoolean(ranges[i] == null);
            if (ranges[i] == null) {
                continue;
            }
            writeRangeWithoutHeader(out, ranges[i], type);
        }
    }

    /** Deserializes a range object from {@link DataInput}. */
    public static ValueRange<?> readRange(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();
        if (isNull) {
            return null;
        }
        ColumnType type = ColumnType.values()[in.readInt()];
        return readRangeWithoutHeader(in, type);
    }

    private static ValueRange<?> readRangeWithoutHeader(DataInput in, ColumnType type) throws IOException {
        boolean isStartNull = in.readBoolean();
        boolean isEndNull = in.readBoolean();
        switch (type) {
        case BIGINT: return new ValueRange<Long>(isStartNull ? null : in.readLong(), isEndNull ? null : in.readLong());
        case BOOLEAN: return new ValueRange<Boolean>(isStartNull ? null : in.readBoolean(), isEndNull ? null : in.readBoolean());
        case DOUBLE: return new ValueRange<Double>(isStartNull ? null : in.readDouble(), isEndNull ? null : in.readDouble());
        case FLOAT: return new ValueRange<Float>(isStartNull ? null : in.readFloat(), isEndNull ? null : in.readFloat());
        case INTEGER: return new ValueRange<Integer>(isStartNull ? null : in.readInt(), isEndNull ? null : in.readInt());
        case SMALLINT: return new ValueRange<Short>(isStartNull ? null : in.readShort(), isEndNull ? null : in.readShort());
        case TINYINT: return new ValueRange<Byte>(isStartNull ? null : in.readByte(), isEndNull ? null : in.readByte());
        case VARCHAR: return new ValueRange<String>(isStartNull ? null : in.readUTF(), isEndNull ? null : in.readUTF());
        default: throw new IllegalArgumentException("Cannot deserialize this type:" + type);
        }
    }
    /** Deserializes an array of range objects from {@link DataInput}. */
    public static ValueRange<?>[] readRanges(DataInput in) throws IOException {
        boolean isArrayNull = in.readBoolean();
        if (isArrayNull) {
            return null;
        }
        
        ColumnType type = ColumnType.values()[in.readInt()];
        int count = in.readInt();
        ValueRange<?>[] array = new ValueRange<?>[count];
        for (int i = 0; i < count; ++i) {
            boolean isNull = in.readBoolean();
            if (!isNull) {
                array[i] = readRangeWithoutHeader(in, type);
            }
        }
        return array;
    }
}
