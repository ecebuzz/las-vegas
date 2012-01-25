package edu.brown.lasvegas;

/**
 * Defines possible status of {@link LVReplicaPartition}.
 */
public enum ReplicaPartitionStatus {
    /** ready for querying.*/
    OK,
    /** this partition has no tuples thus no files (note that it's a valid status).*/
    EMPTY,
    /** the containing node failed or the data file was corrupted.*/
    LOST,
    /** being recovered or created.*/
    BEING_RECOVERED,
    /** kind of null. */
    INVALID,
}
