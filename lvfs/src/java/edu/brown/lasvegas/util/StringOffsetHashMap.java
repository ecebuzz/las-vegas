package edu.brown.lasvegas.util;

/**
 * A hash map with char array keys and Object values.
 * Similar to HashMap, but this receives a giant String and offset/length in it.
 */
public class StringOffsetHashMap<V> {
    /** same logic as String.hashCode(). */
    private static int hashCharArray(String chunk, int offset, int length) {
        int h = 0;
        int off = offset;
        for (int i = 0; i < length; i++) {
            h = 31 * h + chunk.charAt(off++);
        }
        return h;
    }

    /**
     * The hash table data.
     */
    private Entry<V> table[];

    /**
     * The total number of entries in the hash table.
     */
    private int count;

    /**
     * The table is rehashed when its size exceeds this threshold. (The value of this field is (int)(capacity * loadFactor).)
     * 
     * @serial
     */
    private int threshold;

    /**
     * The load factor for the hashtable.
     * 
     * @serial
     */
    private float loadFactor;

    /**
     * <p>
     * Innerclass that acts as a datastructure to create a new entry in the table.
     * </p>
     */
    public static class Entry<V> {
        private final char[] key;
        private final int hash;
        private V value;
        private Entry<V> next;

        public char[] getKey() {
            return key;
        }

        public int getHash() {
            return hash;
        }

        public V getValue() {
            return value;
        }

        public Entry<V> getNext() {
            return next;
        }

        /**
         * <p>
         * Create a new entry with the given values.
         * </p>
         * 
         * @param key
         *            The key used to enter this in the table
         * @param value
         *            The value for this key
         * @param next
         *            A reference to the next entry in the table
         */
        private Entry(char[] key, int hash, V value, Entry<V> next) {
            this.key = key;
            this.hash = hash;
            this.value = value;
            this.next = next;
        }
    }

    /**
     * <p>
     * Constructs a new, empty hashtable with a default capacity and load factor, which is <code>20</code> and <code>0.75</code> respectively.
     * </p>
     */
    public StringOffsetHashMap() {
        this(20, 0.75f);
    }

    /**
     * <p>
     * Constructs a new, empty hashtable with the specified initial capacity and default load factor, which is <code>0.75</code>.
     * </p>
     * 
     * @param initialCapacity
     *            the initial capacity of the hashtable.
     * @throws IllegalArgumentException
     *             if the initial capacity is less than zero.
     */
    public StringOffsetHashMap(int initialCapacity) {
        this(initialCapacity, 0.75f);
    }

    /**
     * <p>
     * Constructs a new, empty hashtable with the specified initial capacity and the specified load factor.
     * </p>
     * 
     * @param initialCapacity
     *            the initial capacity of the hashtable.
     * @param loadFactor
     *            the load factor of the hashtable.
     * @throws IllegalArgumentException
     *             if the initial capacity is less than zero, or if the load factor is nonpositive.
     */
    @SuppressWarnings("unchecked")
    public StringOffsetHashMap(int initialCapacity, float loadFactor) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
        }
        if (loadFactor <= 0) {
            throw new IllegalArgumentException("Illegal Load: " + loadFactor);
        }
        if (initialCapacity == 0) {
            initialCapacity = 1;
        }

        this.loadFactor = loadFactor;
        table = (Entry<V>[]) new Entry[initialCapacity];
        threshold = (int) (initialCapacity * loadFactor);
    }

    /**
     * <p>
     * Returns the number of keys in this hashtable.
     * </p>
     * 
     * @return the number of keys in this hashtable.
     */
    public int size() {
        return count;
    }

    /**
     * <p>
     * Tests if this hashtable maps no keys to values.
     * </p>
     * 
     * @return <code>true</code> if this hashtable maps no keys to values; <code>false</code> otherwise.
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * <p>
     * Tests if the specified String is a key in this hashtable.
     * </p>
     * 
     * @param chunk
     * @param offset
     * @param length
     * @return <code>true</code> if and only if the specified object is a key in this hashtable, as determined by the <tt>equals</tt> method; <code>false</code>
     *         otherwise.
     * @see #contains(Object)
     */
    public boolean containsKey(String chunk, int offset, int length) {
        int hash = hashCharArray(chunk, offset, length);
        Entry<V> tab[] = table;
        int index = (hash & 0x7FFFFFFF) % tab.length;
        for (Entry<V> e = tab[index]; e != null; e = e.next) {
            if (e.hash == hash && e.key.length == length) {
                boolean match = true;
                for (int i = 0; i < length; ++i) {
                    if (chunk.charAt(offset + i) != e.key[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * <p>
     * Returns the value to which the specified key is mapped in this map.
     * </p>
     * 
     * @param chunk
     * @param offset
     * @param length
     * @return the value to which the key is mapped in this hashtable; <code>null</code> if the key is not mapped to any value in this hashtable.
     * @see #put(int, Object)
     */
    public V get(String chunk, int offset, int length) {
        int hash = hashCharArray(chunk, offset, length);
        Entry<V> tab[] = table;
        int index = (hash & 0x7FFFFFFF) % tab.length;
        for (Entry<V> e = tab[index]; e != null; e = e.next) {
            if (e.hash == hash && e.key.length == length) {
                boolean match = true;
                for (int i = 0; i < length; ++i) {
                    if (chunk.charAt(offset + i) != e.key[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return e.value;
                }
            }
        }
        return null;
    }

    /**
     * <p>
     * Increases the capacity of and internally reorganizes this hashtable, in order to accommodate and access its entries more efficiently.
     * </p>
     * 
     * <p>
     * This method is called automatically when the number of keys in the hashtable exceeds this hashtable's capacity and load factor.
     * </p>
     */
    private void rehash() {
        int oldCapacity = table.length;
        Entry<V> oldMap[] = table;

        int newCapacity = oldCapacity * 2 + 1;
        @SuppressWarnings("unchecked")
        Entry<V> newMap[] = (Entry<V>[]) new Entry[newCapacity];

        threshold = (int) (newCapacity * loadFactor);
        table = newMap;

        for (int i = oldCapacity; i-- > 0;) {
            for (Entry<V> old = oldMap[i]; old != null;) {
                Entry<V> e = old;
                old = old.next;

                int index = (e.hash & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    /**
     * <p>
     * Maps the specified <code>key</code> to the specified <code>value</code> in this hashtable. The key cannot be <code>null</code>.
     * </p>
     * 
     * <p>
     * The value can be retrieved by calling the <code>get</code> method with a key that is equal to the original key.
     * </p>
     * 
     * @param chunk
     * @param offset
     * @param length
     * @param value
     *            the value.
     * @return the previous value of the specified key in this hashtable, or <code>null</code> if it did not have one.
     * @see #get(int)
     */
    public V put(String chunk, int offset, int length, V value) {
        // Makes sure the key is not already in the hashtable.
        int hash = hashCharArray(chunk, offset, length);
        Entry<V> tab[] = table;
        int index = (hash & 0x7FFFFFFF) % tab.length;
        for (Entry<V> e = tab[index]; e != null; e = e.next) {
            if (e.hash == hash && e.key.length == length) {
                boolean match = true;
                for (int i = 0; i < length; ++i) {
                    if (chunk.charAt(offset + i) != e.key[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    V old = e.value;
                    e.value = value;
                    return old;
                }
            }
        }

        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();

            tab = table;
            index = (hash & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        char[] subChunk = new char[length];
        chunk.getChars(offset, offset + length, subChunk, 0);
        Entry<V> e = new Entry<V>(subChunk, hash, value, tab[index]);
        tab[index] = e;
        count++;
        return null;
    }

    /**
     * <p>
     * Removes the key (and its corresponding value) from this hashtable.
     * </p>
     * 
     * <p>
     * This method does nothing if the key is not present in the hashtable.
     * </p>
     * 
     * @param chunk
     * @param offset
     * @param length
     * @return the value to which the key had been mapped in this hashtable, or <code>null</code> if the key did not have a mapping.
     */
    public V remove(String chunk, int offset, int length) {
        int hash = hashCharArray(chunk, offset, length);
        Entry<V> tab[] = table;
        int index = (hash & 0x7FFFFFFF) % tab.length;
        for (Entry<V> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.hash == hash && e.key.length == length) {
                boolean match = true;
                for (int i = 0; i < length; ++i) {
                    if (chunk.charAt(offset + i) != e.key[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    if (prev != null) {
                        prev.next = e.next;
                    } else {
                        tab[index] = e.next;
                    }
                    count--;
                    V oldValue = e.value;
                    e.value = null;
                    return oldValue;
                }
            }

        }
        return null;
    }

    /**
     * <p>
     * Clears this hashtable so that it contains no keys.
     * </p>
     */
    public synchronized void clear() {
        Entry<V> tab[] = table;
        for (int index = tab.length; --index >= 0;) {
            tab[index] = null;
        }
        count = 0;
    }

    /**
     * @return the table
     */
    public Entry<V>[] getTable() {
        return table;
    }
}
