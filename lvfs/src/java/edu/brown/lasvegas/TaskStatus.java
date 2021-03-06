package edu.brown.lasvegas;

/**
 * Defines possible status of a local task ({@link LVTask}).
 * Not sure this is going to be any different from {@link JobStatus},
 * but it wouldn't hurt to separate them.
 */
public enum TaskStatus {
    /** Just created, not yet started nor requested to start. */
    CREATED,
    /** Has been requested to start the job. */
    START_REQUESTED,
    /** The start-request has been received and the job is being processed. */
    RUNNING,
    /** Successfully finished. */
    DONE,
    /** Exited with an error. */
    ERROR,
    /** Has been requested to cancel the task. */
    CANCEL_REQUESTED,
    /** Canceled. */
    CANCELED,
    /** kind of null. */
    INVALID,
    ;
    
    /**
     * tells if this status is one of "finished" status.
     * Actually, this should be an abstract method of this enum. However,
     * BDB-JE doesn't support storing enum with constant-specific methods,
     * so this is still a static method.
     */
    public static boolean isFinished (TaskStatus status) {
        return status == DONE || status == CANCELED || status == ERROR; 
    }
}
