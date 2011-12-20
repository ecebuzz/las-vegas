package edu.brown.lasvegas;

/** defines the possible status of a table. */
public enum TableStatus {
    /** The table is in a query-able state. */
    OK,
    /** The table is being created. When done, becomes OK. */
    BEING_CREATED,
    /** The table is being dropped. */
    BEING_DROPPED,
    /** The table has been dropped. */
    DROPPED,
    /** kind of null. */
    INVALID,
}