package edu.brown.lasvegas.util;

import java.text.ParseException;

/**
 * This class is a base for efficient text parsing classes which maintain
 * cache of parsed object and prefer to use char array rather than String.
 * 
 * <p>
 * Text parsing classes like DateTimeFormat have two issues while
 * parsing large data. First, it receives a String object each time rather than a reused giant
 * String with its offset and length. This causes one String object instantiation
 * and revoke for each, say, column. Second, it often parses the exact same text
 * over again, creating a new object for each of them.
 * </p>
 * 
 * <p>
 * Instead, this class maintains a Cache (with maximum size to limit the memory consumption)
 * in hashtable and reuses the parsed object. Also, this class can receive a giant String and
 * its offset and length. In some cases, these improvements eliminates most of object creation
 * and dramatically speeds up the parsing.
 * </p>
 */
public abstract class CachedParser<V> {
    private final int maxCacheSize;
    private final StringOffsetHashMap<V> hashmap;
    
    public CachedParser() {
        this (1 << 20, 1 << 12, 0.75f);
    }
    public CachedParser(int maxCacheSize, int hashtableInitialCapacity, float hashtableLoadFactor) {
        this.maxCacheSize = maxCacheSize;
        this.hashmap = new StringOffsetHashMap<V> (hashtableInitialCapacity, hashtableLoadFactor);
    }
    
    public V parse (String chunk, int offset, int length) throws ParseException {
        V value = hashmap.get(chunk, offset, length);
        if (value != null) {
            return value;
        }
        // cache miss! parse it.
        value = parseMiss(chunk, offset, length);
        // cache it. unless the hashmap is too large
        if (hashmap.size() < maxCacheSize) {
            hashmap.put(chunk, offset, length, value);
        }
        return value;
    }
    
    /**
     * This method is called when a cache miss happens.
     */
    protected abstract V parseMiss (String chunk, int offset, int length) throws ParseException;    
}
