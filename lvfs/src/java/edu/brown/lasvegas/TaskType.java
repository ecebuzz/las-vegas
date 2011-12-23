package edu.brown.lasvegas;

/**
 * Defines types of local Tasks ({@link TaskJob}).
 */
public enum TaskType {
    /** Process projection (evaluate expression etc). If it's purely projection (output column itself), this task is not needed. */
    PROJECT,
    /** Apply filtering to column-files of a sub-partition and output the column-files after filtering. */
    FILTER_COLUMN_FILES,
    /** kind of null. */
    INVALID,
}
