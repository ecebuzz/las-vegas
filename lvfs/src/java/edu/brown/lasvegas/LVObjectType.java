package edu.brown.lasvegas;

/**
 * Enumerates types of classes derived from LVObject.
 */
public enum LVObjectType {
    INVALID,
    COLUMN,
    COLUMN_FILE,
    DATABASE,
    FRACTURE,
    RACK,
    RACK_ASSIGNMENT,
    RACK_NODE,
    REPLICA,
    REPLICA_GROUP,
    REPLICA_PARTITION,
    REPLICA_SCHEME,
    SUB_PARTITION_SCHEME,
    TABLE;
}
