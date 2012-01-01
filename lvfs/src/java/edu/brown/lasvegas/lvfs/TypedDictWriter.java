package edu.brown.lasvegas.lvfs;

/**
 * Additional methods for dictionary-compressed column.
 * @param <T> Value type BEFORE compression
 * @param <AT> Array type BEFORE compression 
 */
public interface TypedDictWriter<T extends Comparable<T>, AT> extends TypedWriter<T, AT> {
    /**
     * Returns the finalized dictionary. This method can be only called after {@link #writeFileFooter()}.
     */
    OrderedDictionary<T, AT> getFinalDict ();
}
