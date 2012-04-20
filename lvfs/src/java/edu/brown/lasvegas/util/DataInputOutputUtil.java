package edu.brown.lasvegas.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Misc utility functions about {@link DataInput} and {@link DataOutput}.
 */
public final class DataInputOutputUtil {
    /**
     * Serializes an array of primitive int.
     */
    public static void writeArray(DataOutput out, int[] array) throws IOException {
        out.writeInt(array == null ? -1 : array.length);
        if (array != null) {
            for (int i = 0; i < array.length; ++i) {
            	out.writeInt(array[i]);
            }
        }
    }

    /**
     * Deserializes an array of primitive int.
     */
    public static int[] readIntArray(DataInput in) throws IOException {
        int len = in.readInt();
        assert (len >= -1);
        if (len == -1) {
            return null;
        } else {
            int[] ret = new int[len];
            for (int i = 0; i < len; ++i) {
            	ret[i] = in.readInt();
            }
            return ret;
        }
    }

    /**
     * Serializer/Deserializer for generic Writable array.
     */
    public static abstract class ArraySerializer<T extends Writable> {
    	public abstract T[] allocateArray (int size);
    	public abstract T read(DataInput in) throws IOException;
    	/**
    	 * Serializes an array of Writable.
    	 */
        public final void writeArray(DataOutput out, T[] array) throws IOException {
            out.writeInt(array == null ? -1 : array.length);
            if (array != null) {
                for (int i = 0; i < array.length; ++i) {
                	array[i].write(out);
                }
            }
        }
        /**
         * Deserializes an array of object.
         */
        public final T[] readArray (DataInput in) throws IOException {
            int len = in.readInt();
            assert (len >= -1);
            if (len == -1) {
                return null;
            } else {
                T[] ret = allocateArray(len);
                for (int i = 0; i < len; ++i) {
                	ret[i] = read (in);
                }
                return ret;
            }
        }
    }

    /**
     * Serializer/Deserializer for generic enum array.
     */
    public static abstract class EnumArraySerializer<T extends Enum<T>> {
    	public abstract T[] allocateArray (int size);
    	public abstract T read(DataInput in) throws IOException;
    	/**
    	 * Serializes an array of Writable.
    	 */
        public final void writeArray(DataOutput out, T[] array) throws IOException {
            out.writeInt(array == null ? -1 : array.length);
            if (array != null) {
                for (int i = 0; i < array.length; ++i) {
                	out.writeInt(array[i].ordinal());
                }
            }
        }
        /**
         * Deserializes an array of object.
         */
        public final T[] readArray (DataInput in) throws IOException {
            int len = in.readInt();
            assert (len >= -1);
            if (len == -1) {
                return null;
            } else {
                T[] ret = allocateArray(len);
                for (int i = 0; i < len; ++i) {
                	ret[i] = read (in);
                }
                return ret;
            }
        }
    }
}
