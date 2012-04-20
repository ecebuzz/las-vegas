package edu.brown.lasvegas.tuple;

import java.io.IOException;


/**
 * A wrapper to filter out certain tuples read from TupleReader.
 * Mainly used for testcases or other non-performance-sensitive cases.
 */
public abstract class FilteredTupleReader extends DefaultTupleReader {
	public FilteredTupleReader (TupleReader reader) {
		super (reader.getColumnTypes());
		this.reader = reader;
	}
	
	@Override
	public boolean next() throws IOException {
		while (true) {
			boolean read = reader.next();
			if (!read) {
				return false;
			}
			for (int i = 0; i < columnCount; ++i) {
				currentData[i] = reader.getObject(i);
			}
			if (isFiltered()) {
				continue;
			}
			return true;
		}
	}
	/**
	 * @return whether to ignore the current tuple.
	 */
	protected abstract boolean isFiltered ();

	@Override
	public String getCurrentTupleAsString() {
		return reader.getCurrentTupleAsString();
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	private final TupleReader reader;
}