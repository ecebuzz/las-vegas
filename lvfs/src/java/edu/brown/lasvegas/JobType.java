package edu.brown.lasvegas;

/**
 * Defines types of Jobs ({@link LVJob}).
 */
public enum JobType {
    /** A job to import data into LVFS as a new fracture.*/
    IMPORT_FRACTURE,
    /** A job to merge fractures.*/
    MERGE_FRACTURE,
    /** A job to process a user-issued query.*/
    QUERY,
    /** kind of null. */
    INVALID,
}