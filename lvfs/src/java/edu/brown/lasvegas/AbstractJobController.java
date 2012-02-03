package edu.brown.lasvegas;

import java.io.IOException;
import java.util.SortedMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Base implementation for JobController.
 */
public abstract class AbstractJobController<Param extends JobParameters>
    implements JobController<Param> {
    private static Logger LOG = Logger.getLogger(AbstractJobController.class);

    /**
     * Metadata repository.
     */
    protected final LVMetadataProtocol metaRepo;
    
    protected Param param;
    protected int jobId;
    
    private final long stopMaxWaitMilliseconds;
    private final long taskJoinIntervalMilliseconds;
    private final long taskJoinIntervalOnErrorMilliseconds;

    public AbstractJobController (LVMetadataProtocol metaRepo) throws IOException {
        this (metaRepo, 3000L, 5000L, 500L);
    }
    public AbstractJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        this.metaRepo = metaRepo;
        this.stopMaxWaitMilliseconds = stopMaxWaitMilliseconds;
        this.taskJoinIntervalMilliseconds = taskJoinIntervalMilliseconds;
        this.taskJoinIntervalOnErrorMilliseconds = taskJoinIntervalOnErrorMilliseconds;
    }
    
    // differences between stopRequested and errorEncountered
    // errorEncountered: will cancel all the tasks, but will also wait to see all tasks are actually stopped.
    // stopRequested: will cancel all the tasks, but exit before checking they are actually stopped.
    protected boolean stopRequested = false;
    protected boolean errorEncountered = false;
    protected String errorMessages = "";

    protected boolean stopped = false;
    @Override
    public final boolean isStopped() {
        return stopped;
    }
    @Override
    public final void requestStop() {
        stopRequested = true;
    }

    @Override
    public final void stop () {
        stopRequested = true;
        try {
            metaRepo.updateJobNoReturn(jobId, JobStatus.CANCEL_REQUESTED, null, null);
        } catch (IOException ex) {
            LOG.error("failed to update job status", ex);
        }
        long initTime = System.currentTimeMillis();
        while (!stopped && System.currentTimeMillis() - initTime < stopMaxWaitMilliseconds) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException ex) {
                // don't continue.. someone else might REALLY want to stop everything
                LOG.warn("gave up stopping", ex);
                break;
            }
            
        }
    }
    @Override
    public final LVJob startAsync(Param param) throws IOException {
        init (param);
        Thread thread = new Thread() {
            @Override
            public void run() {
                runInternal();
            }
        };
        thread.start();
        return metaRepo.getJob(jobId);
    }
    @Override
    public final LVJob startSync(Param param) throws IOException {
        init (param);
        runInternal();
        return metaRepo.getJob(jobId);
    }
    
    protected abstract void initDerived () throws IOException;
    protected abstract void runDerived() throws IOException;

    private void init (Param param) throws IOException {
        this.param = param;
        initDerived();
        assert (jobId != 0);
    }
    private void runInternal() {
        try {
            // First, create a job object for the import.
            if (LOG.isInfoEnabled()) {
                LOG.info("running job: " + metaRepo.getJob(jobId));
            }
            metaRepo.updateJobNoReturn(jobId, JobStatus.RUNNING, null, null);
            
            runDerived();
    
            // okay, done!
            // TODO should we delete temporary files now that they are no longer needed?
            if (errorEncountered) {
                metaRepo.updateJobNoReturn(jobId, JobStatus.ERROR, null, errorMessages);
            } else if (stopRequested) {
                metaRepo.updateJobNoReturn(jobId, JobStatus.CANCELED, null, null);
            } else {
                metaRepo.updateJobNoReturn(jobId, JobStatus.DONE, new DoubleWritable(1.0d), null);
            }
        } catch (Exception ex) {
            LOG.error("unexpected error:" + ex.getMessage(), ex);
            errorEncountered = true;
            errorMessages = ex.getMessage();
            try {
                metaRepo.updateJobNoReturn(jobId, JobStatus.ERROR, null, errorMessages);
            } catch (Exception ex2) {
                LOG.error("failed again to report an unexpected error:" + ex.getMessage(), ex);
            }
        }
        stopped = true;
        LOG.info("exit the job.");
    }

    /** request all nodes to cancel ongoing task. */
    private void cancelAllTasks (SortedMap<Integer, LVTask> taskMap) throws IOException {
        LOG.info("cancelling tasks...");
        for (LVTask task : taskMap.values()) {
            if (TaskStatus.isFinished(task.getStatus()) || task.getStatus() == TaskStatus.CANCEL_REQUESTED) {
                continue;
            }
            metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.CANCEL_REQUESTED, null, null, null);
            task.setStatus(TaskStatus.CANCEL_REQUESTED);
        }
    }

    /**
     * wait until all tasks are completed.
     * if any task threw an error, we stop the entire job (as this is a raw local drive, there is no back-up node to recover from a crash)
     */
    protected final void joinTasks (SortedMap<Integer, LVTask> taskMap, double baseProgress, double completedProgress) throws IOException {
        int finishedCount = 0;
        while (!stopRequested && finishedCount != taskMap.size()) {
            try {
                Thread.sleep(errorEncountered ? taskJoinIntervalOnErrorMilliseconds : taskJoinIntervalMilliseconds);
            } catch (InterruptedException ex) {
            }
            LOG.debug("polling the progress of tasks...");
            for (LVTask task : taskMap.values()) {
                if (TaskStatus.isFinished(task.getStatus())) {
                    continue;
                }
                LVTask updated = metaRepo.getTask(task.getTaskId());
                if (updated.getStatus() != task.getStatus() || updated.getProgress() != task.getProgress()) {
                    LOG.info("some change on task progress: " + updated);
                }
                taskMap.put(updated.getTaskId(), updated);
                if (TaskStatus.isFinished(updated.getStatus())) {
                    ++finishedCount;
                    double jobProgress = baseProgress + (completedProgress - baseProgress) * finishedCount / taskMap.size();
                    metaRepo.updateJobNoReturn(jobId, null, new DoubleWritable(jobProgress), null);
                }
                if (updated.getStatus() == TaskStatus.ERROR) {
                    LOG.error("A task reported an error! : " + updated);
                    errorEncountered = true;
                    errorMessages = updated.getErrorMessages();
                    break;
                }
            }
            if (errorEncountered) {
                cancelAllTasks (taskMap);
            }
        }
        if (stopRequested) {
            cancelAllTasks (taskMap);
        }
    }
}
