package edu.brown.lasvegas;

import java.io.IOException;


/**
 * Represents an object to create, start, monitor, and potentially cancel a particular type of jobs.
 * <p>Each job type defined in {@link JobType} has its own job controller implementation.</p>  
 * <p>Some job controller might have to run on the central node while some
 * can run on any node. The derived job controller class clarifies the requirement
 * in class comments.</p>
 * @see LVJob
 */
public interface JobController<Param extends JobParameters> {
    /**
     * Start the job on a new thread. This method is <b>asynchronous</b> and returns as soon as the job has started.
     * @param param parameters for the job.
     * @return {@link LVJob} object created for this job. <b>It's the value as of the method return.</b>
     * @see #stop()
     */
    LVJob startAsync (Param param) throws IOException;

    /**
     * Start the job on the current thread. This method is <b>synchronous</b> and blocks until the completion of the job.
     * @param param parameters for the job.
     * @return {@link LVJob} object created for this job. As this method is synchronous,
     * the returned object is final.
     */
    LVJob startSync (Param param) throws IOException;

    /** forcibly cancel the job and its sub-tasks, immediately stopping this object. */
    void stop ();
    /** gracefully request to cancel the job and its sub-tasks.*/
    void requestStop ();
    /** answers if the controller stopped all internal threads and released resources. */
    boolean isStopped ();
}
