package edu.brown.lasvegas.qe;

/**
 * The level of logging for a query execution task.
 */
public enum TaskLogLevel {
    /** everything. */
    DEBUG,
    /** only info and higher. */
    INFO,
    /** only warnings and higher. */
    WARN,
    /** only errors. */
    ERROR,
    /** no logging. */
    NONE,
    /** kind of null. */
    INVALID,
}
