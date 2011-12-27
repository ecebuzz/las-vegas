package edu.brown.lasvegas.util;

import java.util.Arrays;

/**
 * Sorting functions for primitive key and value types analogous to {@link Arrays}.
 * 
 * <p>The difference is that this class allows sorting keys with values,
 * for example the value person-ID as int sorted by SSN as long.
 * Arrays can do the same with Object and Comparator, but it's super
 * slow and memory-hogging because they are objects, not primitives.<p>
 * 
 * <p>So far, this class assumes the value type is int. If you want another
 * primitive value types, you have to add another set of functions.
 * This is where C++ style template rules.</p>
 * 
 * @see Arrays
 */
public final class KeyValueArrays {
    /** disabled. */
    private KeyValueArrays() {}

    /**
     * Sorts the specified array of long-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(long[])
     */
    public static void sort(long[] keys, int[] values) {
        sort1(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of long-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(long[], int, int)
     */
    public static void sort(long[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort1(keys, values, fromIndex, toIndex-fromIndex);
    }

    /**
     * Sorts the specified array of int-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(int[])
     */
    public static void sort(int[] keys, int[] values) {
        sort1(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of int-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(int[], int, int)
     */
    public static void sort(int[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort1(keys, values, fromIndex, toIndex-fromIndex);
    }

    /**
     * Sorts the specified array of short-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(short[])
     */
    public static void sort(short[] keys, int[] values) {
        sort1(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of short-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(short[], int, int)
     */
    public static void sort(short[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort1(keys, values, fromIndex, toIndex-fromIndex);
    }

    /**
     * Sorts the specified array of byte-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(byte[])
     */
    public static void sort(byte[] keys, int[] values) {
        sort1(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of byte-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(byte[], int, int)
     */
    public static void sort(byte[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort1(keys, values, fromIndex, toIndex-fromIndex);
    }
    
    /**
     * Sorts the specified array of char-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(char[])
     */
    public static void sort(char[] keys, int[] values) {
        sort1(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of char-keys with int-values into ascending numerical order of the keys.
     * @see Arrays#sort(char[], int, int)
     */
    public static void sort(char[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort1(keys, values, fromIndex, toIndex-fromIndex);
    }

    /**
     * Sorts the specified array of double-keys with int-values into ascending numerical order of the keys.
     * <b>Like Arrays, this method moves NaN to the end.
     * Unlike Arrays, on the other hand, it does NOT move negative zero before positive zero. Both zeros are considered same.</b>
     * @see Arrays#sort(double[])
     */
    public static void sort(double[] keys, int[] values) {
        sort2(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of double-keys with int-values into ascending numerical order of the keys.
     * <b>Like Arrays, this method moves NaN to the end.
     * Unlike Arrays, on the other hand, it does NOT move negative zero before positive zero. Both zeros are considered same.</b>
     * @see Arrays#sort(double[], int, int)
     */
    public static void sort(double[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort2(keys, values, fromIndex, toIndex);
    }

    /**
     * Sorts the specified array of float-keys with int-values into ascending numerical order of the keys.
     * <b>Like Arrays, this method moves NaN to the end.
     * Unlike Arrays, on the other hand, it does NOT move negative zero before positive zero. Both zeros are considered same.</b>
     * @see Arrays#sort(float[])
     */
    public static void sort(float[] keys, int[] values) {
        sort2(keys, values, 0, keys.length);
    }

    /**
     * Sorts the given range of the specified array of float-keys with int-values into ascending numerical order of the keys.
     * <b>Like Arrays, this method moves NaN to the end.
     * Unlike Arrays, on the other hand, it does NOT move negative zero before positive zero. Both zeros are considered same.</b>
     * @see Arrays#sort(float[], int, int)
     */
    public static void sort(float[] keys, int[] values, int fromIndex, int toIndex) {
        rangeCheck(keys.length, values.length, fromIndex, toIndex);
        sort2(keys, values, fromIndex, toIndex);
    }

    /**
     * Sorts the specified sub-array of longs into ascending order.
     */
    private static void sort1(long[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        long v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }
    
    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(long keys[], int values[], int a, int b) {
        long k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(long keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed longs.
     */
    private static int med3(long keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }


    /**
     * Sorts the specified sub-array of ints into ascending order.
     */
    private static void sort1(int[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        int v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }
    
    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(int keys[], int values[], int a, int b) {
        int k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(int keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed ints.
     */
    private static int med3(int keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }


    /**
     * Sorts the specified sub-array of shorts into ascending order.
     */
    private static void sort1(short[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        short v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }
    
    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(short keys[], int values[], int a, int b) {
        short k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(short keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed shorts.
     */
    private static int med3(short keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }

    /**
     * Sorts the specified sub-array of bytes into ascending order.
     */
    private static void sort1(byte[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        byte v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }
    
    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(byte keys[], int values[], int a, int b) {
        byte k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(byte keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed bytes.
     */
    private static int med3(byte keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }

    /**
     * Sorts the specified sub-array of chars into ascending order.
     */
    private static void sort1(char[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        char v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }
    
    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(char keys[], int values[], int a, int b) {
        char k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(char keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed chars.
     */
    private static int med3(char keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }
    
    /**
     * Sorts the specified sub-array of bytes into ascending order.
     */
    private static void sort1(double[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        double v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }

    /**
     * Sorts the specified sub-array of floats into ascending order.
     */
    private static void sort1(float[] keys, int[] values, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && keys[j-1]>keys[j]; j--)
                    swap(keys, values, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(keys, l,     l+s, l+2*s);
                m = med3(keys, m-s,   m,   m+s);
                n = med3(keys, n-2*s, n-s, n);
            }
            m = med3(keys, l, m, n); // Mid-size, med of 3
        }
        float v = keys[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            while (b <= c && keys[b] <= v) {
                if (keys[b] == v)
                    swap(keys, values, a++, b);
                b++;
            }
            while (c >= b && keys[c] >= v) {
                if (keys[c] == v)
                    swap(keys, values, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(keys, values, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(keys, values, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(keys, values, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(keys, values, off, s);
        if ((s = d-c) > 1)
            sort1(keys, values, n-s, s);
    }
    
    private static void sort2(double[] keys, int[] values, int fromIndex, int toIndex) {
        // Move any NaN's to end of array.
        int i = fromIndex, n = toIndex;
        while(i < n) {
            if (keys[i] != keys[i]) {
                swap(keys, values, i, --n);
            } else {
                i++;
            }
        }

        // Main sort phase: quicksort everything but the NaN's
        sort1(keys, values, fromIndex, n-fromIndex);
        
        // Unlike Arrays, we consider negative zero same as positive zero
    }


    private static void sort2(float[] keys, int[] values, int fromIndex, int toIndex) {
        // Move any NaN's to end of array.
        int i = fromIndex, n = toIndex;
        while(i < n) {
            if (keys[i] != keys[i]) {
                swap(keys, values, i, --n);
            } else {
                i++;
            }
        }

        // Main sort phase: quicksort everything but the NaN's
        sort1(keys, values, fromIndex, n-fromIndex);
        
        // Unlike Arrays, we consider negative zero same as positive zero
    }
    
    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(double keys[], int values[], int a, int b) {
        double k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(double keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed doubles.
     */
    private static int med3(double keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }

    /**
     * Swaps key/value[a] with key/value[b].
     */
    private static void swap(float keys[], int values[], int a, int b) {
        float k = keys[a];
        keys[a] = keys[b];
        keys[b] = k;
        int v = values[a];
        values[a] = values[b];
        values[b] = v;
    }

    /**
     * Swaps key/value[a .. (a+n-1)] with key/value[b .. (b+n-1)].
     */
    private static void vecswap(float keys[], int values[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(keys, values, a, b);
    }

    /**
     * Returns the index of the median of the three indexed floats.
     */
    private static int med3(float keys[], int a, int b, int c) {
        return (keys[a] < keys[b] ?
                (keys[b] < keys[c] ? b : keys[a] < keys[c] ? c : a) :
                (keys[b] > keys[c] ? b : keys[a] > keys[c] ? c : a));
    }
    
    /**
     * Check that fromIndex and toIndex are in range, and throw an
     * appropriate exception if they aren't.
     */
    private static void rangeCheck(int keyArrayLen, int valueArrayLen, int fromIndex, int toIndex) {
        if (keyArrayLen != valueArrayLen) {
            throw new IllegalArgumentException("keyArrayLen(" + keyArrayLen +
                            ") != valueArrayLen(" + valueArrayLen+")");
        }
        if (fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex +
                       ") > toIndex(" + toIndex+")");
        if (fromIndex < 0)
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        if (toIndex > keyArrayLen)
            throw new ArrayIndexOutOfBoundsException(toIndex);
    }
}
