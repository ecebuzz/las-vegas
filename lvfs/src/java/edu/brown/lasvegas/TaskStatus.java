package edu.brown.lasvegas;

/**
 * Defines possible status of a local task ({@link LVTask}).
 * Not sure this is going to be any different from {@link JobStatus},
 * but it wouldn't hurt to separate them.
 */
public enum TaskStatus {
    /** Just created, not yet started nor requested to start. */
    CREATED { public boolean isFinished() { return false; } },
    /** Has been requested to start the job. */
    REQUESTED { public boolean isFinished() { return false; } },
    /** The start-request has been received and the job is being processed. */
    RUNNING { public boolean isFinished() { return false; } },
    /** Successfully finished. */
    DONE { public boolean isFinished() { return true; } },
    /** Exited with an error. */
    ERROR { public boolean isFinished() { return true; } },
    /** Canceled per user request. */
    CANCELED { public boolean isFinished() { return true; } },
    /** kind of null. */
    INVALID { public boolean isFinished() { return false; } },
    ;
    
    /** tells if this status is one of "finished" status.*/
    public abstract boolean isFinished ();
}
