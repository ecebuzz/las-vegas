package edu.brown.lasvegas;

/**
 * Defines type of compression scheme for a column file.
 * This is part of <b>physical</b> data schemes.
 */
public class LVColumnCompressionTypes {
    /** No compression. */
    public static final int NONE = 0;
    /** Dictionary encoding. great for few-valued columns. */
    public static final int DICTIONARY = 1;
    /** Run length encoding. great for sorted data. */
    public static final int RLE = 2;
    /** Null suppression encoding. for integer values with lots of leading NULLs. */
    public static final int NULL_SUPPRESS = 3;
    /**
     * Snappy, a general yet lightweight compression.
     * This is the _only_ general compression because we don't support
     * any heavy-weight general compressions like GZIP.
     * We use snappy because it's way faster than others (including LZO)
     * at the cost of slightly worse compression ratio.
     * 
     * <a href="http://code.google.com/p/snappy">snappy</a>
     * <a href="http://code.google.com/p/snappy-java">snappy-java</a>
     */
    public static final int SNAPPY = 4;
    
    private LVColumnCompressionTypes () {}
}
