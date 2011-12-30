package edu.brown.lasvegas.lvfs;

/**
 * Block compression writer such as snappy/gzip compressed files.
 */
public interface TypedBlockCmpWriter<T extends Comparable<T>, AT> extends TypedWriter<T, AT> {
    /**
     * Returns the file size in bytes without compression. This might be approximate. Should be
     * used only for statistics
     */
    long getTotalUncompressedSize ();
}
