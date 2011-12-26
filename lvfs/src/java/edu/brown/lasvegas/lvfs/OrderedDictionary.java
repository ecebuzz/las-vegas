package edu.brown.lasvegas.lvfs;

/**
 * Represents a order-preserving dictionary loaded in-memory for dictionary compression.
 * 
 * <p>All dictionaries are order-preserving, meaning a compressed value keeps the
 * less-than and greater-than relationship (of course in addition to equal-to).
 * For example, if the compressed value of a column is 10 and another compressed value
 * if 20, the decompressed value of former is strictly smaller than that of latter.</p>
 * 
 * <p>Compressed values are 1/2/4 signed integers (byte/short/int). To exploit (literally)
 * the last bit, the dictionary numbering starts from negative values. For example,
 * in 2-byte dictionary, the compressed value are -32768,-32767,... Be VERY careful
 * dealing with this if you call {@link #getDictionary()} and directly use the internal
 * dictionary data. Usually, the conversion is done by {@link #convertCompressedValueToDictionaryIndex(int)}.</p>
 */
public interface OrderedDictionary<T extends Comparable<T>> {
    /**
     * Returns the internal dictionary. Use this for a batch access like
     * dictionary-merging (when merging two dictionary-compressed files).
     * However, be VERY careful about the dictionary numbering. See the class comment.
     */
    T[] getDictionary ();

    /** Returns the byte size of compressed values. 1/2/4 only */
    public byte getBytesPerEntry ();

    /**
     * Compresses the given value with this dictionary.
     * @return compressed value. null if the value does not exist in this dictionary
     */
    public Integer compress (T value);

    /**
     * Returns the decompressed value corresponding to the given compressed value.
     */
    public T decompress (int compresedValue);

    /**
     * Given a compressed value in signed integer, returns the
     * corresponding array index in dict.
     */
    public int convertCompressedValueToDictionaryIndex (int compresedValue);

    /**
     * Given an array index in dict, returns a compressed value in signed integer.
     */
    public int convertDictionaryIndexToCompressedValue (int dictionaryIndex);
}
