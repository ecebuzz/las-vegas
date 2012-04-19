package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.IOException;

import edu.brown.lasvegas.util.DataInputOutputUtil.EnumArraySerializer;

/**
 * Array serializer for {@link CompressionType}.
 */
public class ColumnTypeArraySerializer extends EnumArraySerializer<ColumnType> {
	@Override
	public ColumnType[] allocateArray(int size) {
		return new ColumnType[size];
	}
	@Override
	public ColumnType read(DataInput in) throws IOException {
		return ColumnType.values()[in.readInt()];
	}
}
