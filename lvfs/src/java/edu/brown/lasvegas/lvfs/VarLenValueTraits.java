package edu.brown.lasvegas.lvfs;

import edu.brown.lasvegas.traits.ValueTraits;

/**
 * Functor to read/write variable-length java objects.
 * This interface doesn't provide batched read/write because
 * the implementation will be anyway just a loop.
 */
public interface VarLenValueTraits<T extends Comparable<T>> extends ValueTraits<T, T[]> {
}
