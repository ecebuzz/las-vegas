package edu.brown.lasvegas;

/** defines the possible status of a database. */
public enum DatabaseStatus {
    /** The database is in a query-able state. */
    OK,
    /** The database is being dropped. */
    BEING_DROPPED,
    /** kind of null. */
    INVALID,
}