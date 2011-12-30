package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskParameters;
import edu.brown.lasvegas.TaskRunner;
import edu.brown.lasvegas.TaskStatus;

/**
 * Base class for tasks that will run on an LVFS data node.
 */
public abstract class DataTaskRunner<ParamType extends TaskParameters> implements TaskRunner {
    private static Logger LOG = Logger.getLogger(DataTaskRunner.class);

    protected DataEngineContext context;

    /** metadata object that describes the task. */
    protected LVTask task;
    /** additional parameters de-serialized from the task object. */
    protected ParamType parameters;

    /** Call this method before running the task. */
    @SuppressWarnings("unchecked")
    public void init (DataEngineContext context, LVTask task) throws IOException {
        assert (task != null);
        this.context = context;
        this.task = task;
        TaskParameters deserialized = task.deserializeParameters();
        this.parameters = (ParamType) deserialized;
    }
    
    @Override
    public final void run() {
        try {
            checkTaskCanceled ();
            String[] outputFilePaths = runDataTask();
            context.metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.DONE, null, outputFilePaths, null);
        } catch (Exception ex) {
            if (ex instanceof TaskCanceledException) {
                LOG.info("task canceled");
                try {
                    context.metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.CANCELED, null, null, null);
                } catch (IOException ex2) {
                    LOG.error("failed to update task status", ex2);
                }
            } else {
                LOG.error("exception in runDerived()", ex);
                try {
                    context.metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.ERROR, null, null, ex.getMessage());
                } catch (IOException ex2) {
                    LOG.error("failed to update task status", ex2);
                }
            }
        }
    }
    
    /**
     * Implement the actual task in derived classes.
     * @return output file paths
     */
    protected abstract String[] runDataTask () throws Exception;

    /**
     * The implementation of runDerived() should occasionally (not too often!) call this method
     * to terminate its work.
     * @throws TaskCanceledException when the currently running task has been requested to terminate
     */
    protected final void checkTaskCanceled() throws TaskCanceledException {
        if (isTaskCanceled()) {
            throw new TaskCanceledException ();
        }
    }
    /** this one checks it gracefully, just returning whether it's canceled or not. */
    protected final boolean isTaskCanceled() {
        try {
            task = context.metaRepo.getTask(task.getTaskId());
            return task.getStatus() == TaskStatus.CANCEL_REQUESTED;
        } catch (IOException ex) {
            LOG.error("failed to check task status", ex);
            return true;
        }
    }

    /** thrown from runDerived when the task is canceled. */
    public static class TaskCanceledException extends Exception {
        public static final long serialVersionUID = 1L;
    }
}
