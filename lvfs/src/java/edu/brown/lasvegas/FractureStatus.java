package edu.brown.lasvegas;

/** defines the possible status of a fracture. */
public enum FractureStatus {
    /** The fracture is in a query-able state. */
    OK,
    /** The fracture is being created or dropped. */
    INACTIVE,
    /** kind of null. */
    INVALID,
}