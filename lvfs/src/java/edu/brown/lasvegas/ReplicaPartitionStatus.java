package edu.brown.lasvegas;

/**
 * Defines possible status of {@link LVReplicaPartition}.
 */
public enum ReplicaPartitionStatus {
    /** ready for querying.*/
    OK,
    /** the containing node failed or the data file was corrupted.*/
    LOST,
    /** being recovered or created.*/
    BEING_RECOVERED,
}
