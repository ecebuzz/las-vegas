package edu.brown.lasvegas.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * A set of slightly optimized value-parsing functions such as Integer#parseInt().
 * These are useful while parsing a huge text data.
 */
public final class ParseUtil {
    /**
     * Same as Boolean#parseBoolean() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Boolean#parseBoolean(String)
     */
    public static boolean parseBoolean(final String str, final int offset, final int len)
        throws NumberFormatException {
        if (len != 4) return false;
        char c;
        c = str.charAt(offset + 0);
        if (c != 't' && c != 'T') return false;
        c = str.charAt(offset + 1);
        if (c != 'r' && c != 'R') return false;
        c = str.charAt(offset + 2);
        if (c != 'u' && c != 'U') return false;
        c = str.charAt(offset + 3);
        if (c != 'e' && c != 'E') return false;
        return true;
    }

    /**
     * Same as Byte#parseByte() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Byte#parseByte(String)
     */
    public static byte parseByte(final String str, final int offset, final int len)
        throws NumberFormatException {
        return parseByte (str, offset, len, 10);
    }
    
    /**
     * Same as Byte#parseByte() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Byte#parseByte(String, int)
     */
    public static byte parseByte(final String str, final int offset, final int len, final int radix)
        throws NumberFormatException {
        int i = parseInt(str, offset, len, radix);
        if (i < Byte.MIN_VALUE || i > Byte.MAX_VALUE)
            throw forInputString("Value out of range", str, offset, len);
        return (byte)i;
    }

    /**
     * Same as Short#parseShort() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Short#parseShort(String)
     */
    public static short parseShort(final String str, final int offset, final int len)
        throws NumberFormatException {
        return parseShort (str, offset, len, 10);
    }
    
    /**
     * Same as Short#parseShort() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Short#parseShort(String, int)
     */
    public static short parseShort(final String str, final int offset, final int len, final int radix)
        throws NumberFormatException {
        int i = parseInt(str, offset, len, radix);
        if (i < Short.MIN_VALUE || i > Short.MAX_VALUE)
            throw forInputString("Value out of range", str, offset, len);
        return (short)i;
    }

    /**
     * Same as Integer#parseInt() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Integer#parseInt(String)
     */
    public static int parseInt(final String str, final int offset, final int len)
        throws NumberFormatException {
        return parseInt (str, offset, len, 10);
    }
    /**
     * Same as Integer#parseInt() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Integer#parseInt(String, int)
     */
    public static int parseInt(final String str, final int offset, final int len, final int radix)
        throws NumberFormatException
    {
        checkInput(str, offset, len, radix);

        int result = 0;
        boolean negative = false;
        int i = 0;
        int limit = -Integer.MAX_VALUE;
        int multmin;
        int digit;

        char firstChar = str.charAt(offset);
        if (firstChar < '0') { // Possible leading "-"
            if (firstChar == '-') {
                negative = true;
                limit = Integer.MIN_VALUE;
            } else
                throw forInputString(str, offset, len);

            if (len == 1) // Cannot have lone "-"
                throw forInputString(str, offset, len);
            i++;
        }
        multmin = limit / radix;
        while (i < len) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            digit = Character.digit(str.charAt(offset + (i++)),radix);
            if (digit < 0) {
                throw forInputString(str, offset, len);
            }
            if (result < multmin) {
                throw forInputString(str, offset, len);
            }
            result *= radix;
            if (result < limit + digit) {
                throw forInputString(str, offset, len);
            }
            result -= digit;
        }
        return negative ? result : -result;
    }


    /**
     * Same as Long#parseLong() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Long#parseLong(String)
     */
    public static long parseLong(final String str, final int offset, final int len)
        throws NumberFormatException {
        return parseLong (str, offset, len, 10);
    }
    
    /**
     * Same as Long#parseLong() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Long#parseLong(String, int)
     */
    public static long parseLong(final String str, final int offset, final int len, final int radix)
        throws NumberFormatException {
        checkInput(str, offset, len, radix);

        long result = 0;
        boolean negative = false;
        int i = 0;
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;

        char firstChar = str.charAt(offset);
        if (firstChar < '0') { // Possible leading "-"
            if (firstChar == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
            } else
                throw forInputString(str, offset, len);

            if (len == 1) // Cannot have lone "-"
                throw forInputString(str, offset, len);
            i++;
        }
        multmin = limit / radix;
        while (i < len) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            digit = Character.digit(str.charAt(offset + (i++)),radix);
            if (digit < 0) {
                throw forInputString(str, offset, len);
            }
            if (result < multmin) {
                throw forInputString(str, offset, len);
            }
            result *= radix;
            if (result < limit + digit) {
                throw forInputString(str, offset, len);
            }
            result -= digit;
        }
        return negative ? result : -result;
    }

    /**
     * Same as Float#parseFloat() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Float#parseFloat(String)
     */
    public static float parseFloat(final String str, final int offset, final int len)
    throws NumberFormatException {
        if (str == null) {
            throw new NumberFormatException("null");
        }
        if (offset < 0 || offset >= str.length() || len <= 0 || offset + len > str.length()) {
            throw forInputString("offset/len out of range", str, offset, len);
        }
        // TODO not implemented an optimized version yet
        return Float.parseFloat(str.substring(offset, offset + len));
    }
    
    /**
     * Same as Double#parseDouble() except that this function receives a subset of a string
     * to avoid creating a redundant String object. 
     * @see Double#parseDouble(String)
     */
    public static double parseDouble(final String str, final int offset, final int len)
    throws NumberFormatException {
        if (str == null) {
            throw new NumberFormatException("null");
        }
        if (offset < 0 || offset >= str.length() || len <= 0 || offset + len > str.length()) {
            throw forInputString("offset/len out of range", str, offset, len);
        }
        // TODO not implemented an optimized version yet
        return Double.parseDouble(str.substring(offset, offset + len));
    }
    
    /* not yet done. when completed, this should be much faster than sun.java.FloatingDecimal#readJavaFormatString()+float/doubleValue().
     * However, float/double columns are much less common (probably), so this has less priority.
    public static float parseFloat(final String str, final int offset, final int len)
        throws NumberFormatException {
        if (str == null) {
            throw new NumberFormatException("null");
        }
        if (offset < 0 || offset >= str.length() || len <= 0 || offset + len > str.length()) {
            throw forInputString("offset/len out of range", str, offset, len);
        }
        
        // trim whitespace
        int off = offset;
        int end = offset + len;
        while (str.charAt(off) == ' ') {
            ++off;
            if (off == end) {
                throw forInputString("empty", str, offset, len);
            }
        }
        while (str.charAt(end - 1) == ' ') {
            --end;
            if (off == end) {
                throw forInputString("empty", str, offset, len);
            }
        }
        
        boolean isNegative = false;
        boolean signSeen   = false;
        int     decExp;
        char c;

        switch (c = str.charAt(off) ){
        case '-':
            isNegative = true;
            //FALLTHROUGH
        case '+':
            ++off;
            c = str.charAt(off);
            signSeen = true;
        }

        if (c == 'N' || c == 'I') { // possible NaN or infinity
            boolean isNan = checkIsNan(str, offset, len, off, end); // this also does error checks
            return (isNan ? Float.NaN // NaN has no sign
                    : isNegative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
        }
        
        // TODO implement
return 0;
    }
    
    
    private static final char infinity[] = { 'I', 'n', 'f', 'i', 'n', 'i', 't', 'y' };
    private static final char notANumber[] = { 'N', 'a', 'N' };
    private static final char zero[] = { '0', '0', '0', '0', '0', '0', '0', '0' };
    private static boolean checkIsNan (final String str, final int offset, final int len, int off, final int end) throws NumberFormatException {
        boolean potentialNaN = false;
        char targetChars[] = null;  // char array of "NaN" or "Infinity"

        char c = str.charAt(off);
        if(c == 'N') {
            targetChars = notANumber;
            potentialNaN = true;
        } else {
            assert (c == 'I');
            targetChars = infinity;
        }

        // compare Input string to "NaN" or "Infinity"
        int j = 0;
        while(off < end && j < targetChars.length) {
            if(str.charAt(off) == targetChars[j]) {
                ++off; ++j;
            }
            else // something is amiss, throw exception
                throw forInputString(str, offset, len);
        }

        // For the candidate string to be a NaN or infinity,
        // all characters in input string and target char[]
        // must be matched ==> j must equal targetChars.length
        // and i must equal l
        if( (j == targetChars.length) && (off == end) ) { // return NaN or infinity
            return potentialNaN;
        }
        else { // something went wrong, throw exception
            throw forInputString(str, offset, len);
        }
    }
    
    */
    
    
    public static class DateCachedParser extends CachedParser<Date> {
        public DateCachedParser (DateFormat format) {
            super ();
            this.format = format;
        }
        public DateCachedParser (DateFormat format, int maxCacheSize, int hashtableInitialCapacity, float hashtableLoadFactor) {
            super (maxCacheSize, hashtableInitialCapacity, hashtableLoadFactor);
            this.format = format;
        }
        private final DateFormat format;
        @Override
        protected Date parseMiss(String chunk, int offset, int length) throws ParseException {
            return format.parse(chunk.substring(offset, offset + length));
        }
    }

    private static void checkInput(final String str, final int offset, final int len, final int radix) {
        if (str == null) {
            throw new NumberFormatException("null");
        }
        if (offset < 0 || offset >= str.length() || len <= 0 || offset + len > str.length()) {
            throw forInputString("offset/len out of range", str, offset, len);
        }

        if (radix < Character.MIN_RADIX) {
            throw new NumberFormatException("radix " + radix +
                                            " less than Character.MIN_RADIX");
        }

        if (radix > Character.MAX_RADIX) {
            throw new NumberFormatException("radix " + radix +
                                            " greater than Character.MAX_RADIX");
        }
    }
    private static NumberFormatException forInputString (final String str, final int offset, final int len) {
        return forInputString(null, str, offset, len);
    }
    private static NumberFormatException forInputString (final String message, final String str, final int offset, final int len) {
        return new NumberFormatException((message == null ? "" : message + ": ") + "inputString=" + str + "[offset=" + offset + ",len=" + len + "]");
    }
    
    private ParseUtil() {}
}
