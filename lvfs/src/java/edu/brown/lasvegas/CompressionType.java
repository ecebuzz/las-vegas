package edu.brown.lasvegas;

import java.util.zip.Deflater;

/**
 * Defines type of compression scheme for a column file.
 * This is part of <b>physical</b> data schemes.
 */
public enum CompressionType {
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
     * We use snappy because it's way faster than others (including LZO)
     * at the cost of slightly worse compression ratio.
     * 
     * See these:
     * <a href="http://code.google.com/p/snappy">snappy</a>
     * <a href="http://code.google.com/p/snappy-java">snappy-java</a>
     */
    SNAPPY,
    /**
     * GZip with {@link Deflater#BEST_COMPRESSION} level.
     * This provides somewhat better compression than Snappy, but compression/decompression
     * is way way slower.
     * <p>Note: We don't provide GZIP_DEFAULT_COMPRESSION or GZIP_BEST_SPEED because
     * the criteria is what Snappy is for.</p> 
     */
    GZIP_BEST_COMPRESSION,
}