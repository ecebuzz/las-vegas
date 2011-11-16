package edu.brown.lasvegas;

/**
 * Defines type of compression scheme for a column file.
 * This is part of <b>physical</b> data schemes.
 */
public enum LVCompressionType {
    /** No compression. */
    NONE,
    /** Dictionary encoding. great for few-valued columns. */
    DICTIONARY,
    /** Run length encoding. great for sorted data. */
    RLE,
    /** Null suppression encoding. for values with lots of leading NULLs. */
    NULL_SUPPRESS,
    /**
     * Snappy, a general yet lightweight compression.
     * This is the _only_ general compression because we don't support
     * any heavy-weight general compressions like GZIP.
     * We use snappy because it's way faster than others (including LZO)
     * at the cost of slightly worse compression ratio.
     * 
     * See these:
     * <a href="http://code.google.com/p/snappy">snappy</a>
     * <a href="http://code.google.com/p/snappy-java">snappy-java</a>
     */
    SNAPPY,
}