package edu.brown.lasvegas.util;

import java.io.DataInput;

import edu.brown.lasvegas.util.DataInputOutputUtil.ArraySerializer;

/**
 * Array serializer for {@link ValueRange}.
 */
public class ValueRangeArraySerializer extends ArraySerializer<ValueRange> {
	@Override
	public ValueRange[] allocateArray(int size) {
		return new ValueRange[size];
	}
	@Override
	public ValueRange read(DataInput in) {
		return read (in);
	}
}