package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.traits.FixLenValueTraits;

/**
 * Additional methods for dictionary-compressed column.
 * @param <T> Value type BEFORE compression (in other words, after de-compression)
 * @param <AT> Array type BEFORE compression (in other words, after de-compression)
 * @param <CT> Value type AFTER compression (in other words, before de-compression). byte/short/int.
 * @param <CAT> Array type AFTER compression (in other words, before de-compression). byte[]/short[]/int[].
 */
public interface TypedDictReader<T extends Comparable<T>, AT, CT extends Number & Comparable<CT>, CAT> extends TypedReader<T, AT> {
    /**
     * Explicitly loads the dictionary file to this reader.
     * Other than this method call, the dictionary is automatically loaded on readValue()/readValues()/getDict().
     * readCompressedXxx() does not load it.
     */
    void loadDict () throws IOException;

    /**
     * Returns the dictionary.
     */
    OrderedDictionary<T, AT> getDict () throws IOException;

    /**
     * Returns the integer reader object without de-compression. Used for in-situ data processing.
     */
    TypedReader<CT, CAT> getCompressedReader ();
    
    /**
     * Returns the traits instance for dictionary-compressed data type (byte/short/int).
     */
    FixLenValueTraits<CT, CAT> getCompressedValueTraits ();

    /**
     * Same as {@link #readValue()}, but returns the dictionary-compressed integer value.
     */
    CT readCompressedValue () throws IOException;
    
    /**
     * Same as {@link #readValues(Object, int, int)}, but returns the dictionary-compressed integer values.
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values actually read
     */
    int readCompressedValues (CAT buffer, int off, int len) throws IOException;


    /**
     * Same as {@link #readValue()}, but returns the dictionary-compressed integer value.
     * This version returns a 4-bytes integer regardless of CT/CAT.
     */
    int readCompressedValueInt () throws IOException;
    
    /**
     * Same as {@link #readValues(Object, int, int)}, but returns the dictionary-compressed integer values.
     * This version returns a 4-bytes integer regardless of CT/CAT. It's easier to work with,
     * but there might be a slight overhead if the underlying compressed type is smaller than int (eg short).
     * @param buffer the buffer to receive results
     * @param off offset of the buffer
     * @param len maximum number of values to read
     * @return number of values actually read
     */
    int readCompressedValuesInt (int[] buffer, int off, int len) throws IOException;
}
