package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.IOException;

import edu.brown.lasvegas.util.DataInputOutputUtil.EnumArraySerializer;

/**
 * Array serializer for {@link CompressionType}.
 */
public class CompressionTypeArraySerializer extends EnumArraySerializer<CompressionType> {
	@Override
	public CompressionType[] allocateArray(int size) {
		return new CompressionType[size];
	}
	@Override
	public CompressionType read(DataInput in) throws IOException {
		return CompressionType.values()[in.readInt()];
	}
}
