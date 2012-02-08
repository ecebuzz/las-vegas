package edu.brown.lasvegas.util;

/**
 * A hash set for int values which is even faster than IntIntHashMap.
 */
public class IntHashSet {
    /**
     * The hash table data.
     */
    private Entry table[];

    /**
     * The total number of entries in the hash table.
     */
    private int count;

    /**
     * The table is rehashed when its size exceeds this threshold. (The value of this field is (int)(capacity * loadFactor).)
     */
    private int threshold;

    /**
     * The load factor for the hashtable.
     */
    private float loadFactor;

    /**
     * <p>
     * Innerclass that acts as a datastructure to create a new entry in the table.
     * </p>
     */
    public static class Entry {
        private final int key;
        private Entry next;

        public int getKey() {
            return key;
        }

        public Entry getNext() {
            return next;
        }

        /**
         * <p>
         * Create a new entry with the given values.
         * </p>
         * 
         * @param key
         *            The key used to enter this in the table
         * @param next
         *            A reference to the next entry in the table
         */
        private Entry(int key, Entry next) {
            this.key = key;
            this.next = next;
        }
    }

    /**
     * <p>
     * Constructs a new, empty hashtable with a default capacity and load factor, which is <code>20</code> and <code>0.75</code> respectively.
     * </p>
     */
    public IntHashSet() {
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
    public IntHashSet(int initialCapacity) {
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
    public IntHashSet(int initialCapacity, float loadFactor) {
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
        table = new Entry[initialCapacity];
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
     * Tests if the specified object is a key in this hashtable.
     * </p>
     * 
     * @param key
     *            possible key.
     * @return <code>true</code> if and only if the specified value is a key in this hashtable, as determined by the <tt>equals</tt> method; <code>false</code>
     *         otherwise.
     */
    public boolean contains(int key) {
        Entry tab[] = table;
        int index = (key & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.key == key) {
                return true;
            }
        }
        return false;
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
        Entry oldMap[] = table;

        int newCapacity = oldCapacity * 2 + 1;
        Entry newMap[] = new Entry[newCapacity];

        threshold = (int) (newCapacity * loadFactor);
        table = newMap;

        for (int i = oldCapacity; i-- > 0;) {
            for (Entry old = oldMap[i]; old != null;) {
                Entry e = old;
                old = old.next;

                int index = (e.key & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    /**
     * <p>
     * Adds the specified <code>key</code> in this hashtable.
     * </p>
     * 
     * @param key
     *            the hashtable key.
     * @return whether the key was successfully added. true when the key wasn't there, false when already existed.
     */
    public boolean add(int key) {
        // Makes sure the key is not already in the hashtable.
        Entry tab[] = table;
        int index = (key & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.key == key) {
                return false;
            }
        }

        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();

            tab = table;
            index = (key & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        Entry e = new Entry(key, tab[index]);
        tab[index] = e;
        count++;
        return true;
    }

    /**
     * <p>
     * Removes the key from this hashtable.
     * </p>
     * 
     * <p>
     * This method does nothing if the key is not present in the hashtable.
     * </p>
     * 
     * @param key
     *            the key that needs to be removed.
     * @return true if removed.
     */
    public boolean remove(int key) {
        Entry tab[] = table;
        int index = (key & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.key == key) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[index] = e.next;
                }
                count--;
                return true;
            }
        }
        return false;
    }

    /**
     * <p>
     * Clears this hashtable so that it contains no keys.
     * </p>
     */
    public synchronized void clear() {
        Entry tab[] = table;
        for (int index = tab.length; --index >= 0;) {
            tab[index] = null;
        }
        count = 0;
    }

    /**
     * @return the table
     */
    public Entry[] getTable() {
        return table;
    }
}
