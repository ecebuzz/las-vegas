package edu.brown.lasvegas;

/**
 * Defines possible status of {@link LVRack}.
 */
public enum RackStatus {
    /** working correctly.*/
    OK,
    /** the entire rack is out of reach.*/
    LOST,
    /** kind of null. */
    INVALID,
}
