package edu.brown.lasvegas;

/** defines the possible status of a table. */
public enum ColumnStatus {
    /** The column is in a query-able state. */
    OK,
    /** The column is being created. When done, becomes OK. */
    BEING_CREATED,
    /** The column is being dropped. */
    BEING_DROPPED,
    /** The column has been dropped. */
    DROPPED,
}