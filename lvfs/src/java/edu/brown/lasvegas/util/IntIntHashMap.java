package edu.brown.lasvegas.util;

/**
 * A hash map with int keys and int values which is much MUCH faster than HashMap. Also, this class has a special purpose method, {@link #addOrIncrease(int)}.
 * As values are primitive, this uses 'nullValue' instead of null. see {@link #setNullValue(int)}.
 */
public class IntIntHashMap {
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

    /** special value used as null. */
    private int nullValue = -1;

    /**
     * <p>
     * Innerclass that acts as a datastructure to create a new entry in the table.
     * </p>
     */
    public static class Entry {
        private final int key;
        private int value;
        private Entry next;

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
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
         * @param value
         *            The value for this key
         * @param next
         *            A reference to the next entry in the table
         */
        private Entry(int key, int value, Entry next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }

    /**
     * <p>
     * Constructs a new, empty hashtable with a default capacity and load factor, which is <code>20</code> and <code>0.75</code> respectively.
     * </p>
     */
    public IntIntHashMap() {
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
    public IntIntHashMap(int initialCapacity) {
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
    public IntIntHashMap(int initialCapacity, float loadFactor) {
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
    public boolean containsKey(int key) {
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
     * Returns the value to which the specified key is mapped in this map.
     * </p>
     * 
     * @param key
     *            a key in the hashtable.
     * @return the value to which the key is mapped in this hashtable; <code>null</code> if the key is not mapped to any value in this hashtable.
     * @see #put(int, int)
     */
    public int get(int key) {
        Entry tab[] = table;
        int index = (key & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.key == key) {
                return e.value;
            }
        }
        return nullValue;
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
     * Maps the specified <code>key</code> to the specified <code>value</code> in this hashtable.
     * </p>
     * 
     * <p>
     * The value can be retrieved by calling the <code>get</code> method with a key that is equal to the original key.
     * </p>
     * 
     * @param key
     *            the hashtable key.
     * @param value
     *            the value.
     * @see #get(int)
     */
    public void put(int key, int value) {
        // Makes sure the key is not already in the hashtable.
        Entry tab[] = table;
        int index = (key & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.key == key) {
                e.value = value;
                return;
            }
        }

        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();

            tab = table;
            index = (key & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        Entry e = new Entry(key, value, tab[index]);
        tab[index] = e;
        count++;
    }

    /**
     * <p>
     * Increases the value mapped to the specified <code>key</code>. If the key is not mapped yet, <strong>1</strong> is set. This is a special purpose method
     * for counting.
     * </p>
     * 
     * @param key
     *            the hashtable key.
     */
    public void addOrIncrease(int key) {
        // Makes sure the key is not already in the hashtable.
        Entry tab[] = table;
        int index = (key & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.key == key) {
                ++e.value; // increment
                return;
            }
        }

        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();

            tab = table;
            index = (key & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        Entry e = new Entry(key, 1, tab[index]);
        tab[index] = e;
        count++;
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
     * @return the nullValue
     */
    public int getNullValue() {
        return nullValue;
    }

    /**
     * @param nullValue
     *            the nullValue to set
     */
    public void setNullValue(int nullValue) {
        this.nullValue = nullValue;
    }

    /**
     * @return the table
     */
    public Entry[] getTable() {
        return table;
    }
}
