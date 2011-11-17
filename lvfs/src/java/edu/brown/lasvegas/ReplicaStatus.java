package edu.brown.lasvegas;

/**
 * Defines the possible status of a replica (Replicated-Fracture).
 */
public enum ReplicaStatus {
    /** All partitions are alive and can be queried. */
    OK,
    /** Some partition is lost or being recovered. */
    NOT_READY,
}
