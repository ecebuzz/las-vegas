package edu.brown.lasvegas.lvfs;

import java.io.IOException;

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
 * @param <T> Value type
 * @param <AT> Array type 
 */
public interface OrderedDictionary<T extends Comparable<T>, AT> {
    /**
     * Returns the internal dictionary. Use this for a batch access like
     * dictionary-merging (when merging two dictionary-compressed files).
     * However, be VERY careful about the dictionary numbering. See the class comment.
     */
    AT getDictionary ();

    /**
     * Returns the number of entries in the dictionary.
     */
    int getDictionarySize ();

    /** Returns the byte size of compressed values. 1/2/4 only */
    public byte getBytesPerEntry ();

    /**
     * Compresses the given value with this dictionary.
     * @return compressed value. null if the value does not exist in this dictionary
     */
    public Integer compress (T value);
    
    /**
     * Returns a compressed value which represents the largest value in this dictionary
     * that does not exceed the given value. In other words, the given value itself if it exists,
     * but the next smaller value if it does not. Used for range search in the dictionary.
     * @return compressed value. null if there is no value that does not exceed the given value in this dictionary
     */
    public Integer compressLower (T value);
    
    /**
     * Compresses the given array of values altogether (1-byte entry version). This is much more efficient than non-batched version.
     * @param src array of values to compress
     * @param srcOff index of src from which we compress values
     * @param dest array to receive values after compression
     * @param destOff index from which we store compressed values
     * @param len number of values to compress
     */
    public void compressBatch(AT src, int srcOff, byte[] dest, int destOff, int len);

    /**
     * Compresses the given array of values altogether (2-byte entry version). This is much more efficient than non-batched version.
     * @param src array of values to compress
     * @param srcOff index of src from which we compress values
     * @param dest array to receive values after compression
     * @param destOff index from which we store compressed values
     * @param len number of values to compress
     */
    public void compressBatch(AT src, int srcOff, short[] dest, int destOff, int len);

    /**
     * Compresses the given array of values altogether (4-byte entry version). This is much more efficient than non-batched version.
     * @param src array of values to compress
     * @param srcOff index of src from which we compress values
     * @param dest array to receive values after compression
     * @param destOff index from which we store compressed values
     * @param len number of values to compress
     */
    public void compressBatch(AT src, int srcOff, int[] dest, int destOff, int len);

    /**
     * Returns the decompressed value corresponding to the given compressed value.
     */
    public T decompress (int compresedValue);
    
    /**
     * Decompresses the given array of values altogether (1-byte entry version). This is much more efficient than non-batched version.
     * @param src array of values to de-compress
     * @param srcOff index of src from which we de-compress values
     * @param dest array to receive values after de-compression
     * @param destOff index from which we store de-compressed values
     * @param len number of values to de-compress
     * @return actual number of values de-compressed
     */
    public int decompressBatch (byte[] src, int srcOff, AT dest, int destOff, int len);

    /**
     * Decompresses the given array of values altogether (2-byte entry version). This is much more efficient than non-batched version.
     * @param src array of values to de-compress
     * @param srcOff index of src from which we de-compress values
     * @param dest array to receive values after de-compression
     * @param destOff index from which we store de-compressed values
     * @param len number of values to de-compress
     * @return actual number of values de-compressed
     */
    public int decompressBatch (short[] src, int srcOff, AT dest, int destOff, int len);

    /**
     * Decompresses the given array of values altogether (4-byte entry version). This is much more efficient than non-batched version.
     * @param src array of values to de-compress
     * @param srcOff index of src from which we de-compress values
     * @param dest array to receive values after de-compression
     * @param destOff index from which we store de-compressed values
     * @param len number of values to de-compress
     * @return actual number of values de-compressed
     */
    public int decompressBatch (int[] src, int srcOff, AT dest, int destOff, int len);
    
    /**
     * Given a compressed value in signed integer, returns the
     * corresponding array index in dict.
     */
    public int convertCompressedValueToDictionaryIndex (int compresedValue);

    /**
     * Given an array index in dict, returns a compressed value in signed integer.
     */
    public int convertDictionaryIndexToCompressedValue (int dictionaryIndex);

    /**
     * Serializes the dictionary into the given file.
     */
    public void writeToFile (VirtualFile dictFile) throws IOException;
}
