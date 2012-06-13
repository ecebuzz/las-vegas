package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;

/**
 * A thread that runs on LVFS data node to continuously pull
 * new tasks for the node.
 */
public final class DataTaskPollingThread extends Thread {
    private static Logger LOG = Logger.getLogger(DataTaskPollingThread.class);

    /** interval between each polling. */
    public static final String POLLING_INTERVAL_KEY = "lasvegas.server.data.polling.interval";
    /** in msec. */
    public static final long POLLING_INTERVAL_DEFAULT = 1000L;

    /** max concurrent threads to run tasks on this node. */
    public static final String TASK_WORKER_THREADS_KEY = "lasvegas.server.data.task.workers";
    public static final int TASK_WORKER_THREADS_DEFAULT = 2;

    private DataEngineContext context;
    private boolean stopRequested = false;
    private boolean stopped = false;

    /** worker thread pool to run data tasks. */
    private ExecutorService workerPool;

    public DataTaskPollingThread (DataEngineContext context) throws IOException {
        this.context = context;
        LVRackNode node = context.metaRepo.getRackNode(context.nodeId);
        if (node == null) {
            LOG.error("Failed to start a polling thread. This node ID doesn't exist: " + context.nodeId);
            throw new IOException ("Failed to start a polling thread. This node ID doesn't exist: " + context.nodeId);
        }
        LOG.info("starting a data task polling thread on " + node);
        context.metaRepo.updateRackNodeStatus(node, RackNodeStatus.OK);
        int workers = context.conf.getInt(TASK_WORKER_THREADS_KEY, TASK_WORKER_THREADS_DEFAULT);
        workerPool = Executors.newFixedThreadPool(workers);
    }
    
    /**
     * gracefully requests this thread to stop.
     * it will most likely stop soon.
     * Call join() additionally if you want to wait til the end.
     */
    public void shutdown () {
        stopRequested = true;
        try {
            // request each task to stop too
            TaskStatus[] statusToStop = new TaskStatus[]{TaskStatus.START_REQUESTED, TaskStatus.RUNNING};
            for (TaskStatus status : statusToStop) {
                for (LVTask task : context.metaRepo.getAllTasksByNodeAndStatus(context.nodeId, status)) {
                    context.metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.CANCEL_REQUESTED, null, null, null);
                }
            }
        } catch (IOException ex) {
            LOG.error("unexpected exception while shutting down tasks on Node-" + context.nodeId, ex);
        }
        workerPool.shutdown();
        interrupt();
    }
    /** Returns if this thread has stopped. */
    public boolean isStopped () {
        return stopped;
    }
    
    @Override
    public void run() {
        try {
            while (!stopRequested) {
                LOG.trace("polling my task...");
                LVTask[] newTasks = context.metaRepo.getAllTasksByNodeAndStatus(context.nodeId, TaskStatus.START_REQUESTED);
                if (LOG.isDebugEnabled() && newTasks.length > 0) {
                    LOG.debug("going to run " + newTasks.length + " tasks..");
                }
                for (LVTask task : newTasks) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("New Data Task:" + task);
                    }
                    DataTaskRunner<?> runnable = (DataTaskRunner<?>) TaskType.instantiateRunner(task.getType());
                    if (runnable == null) {
                        LOG.error("unexpected task type:" + task);
                        context.metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.ERROR, null, null, "unexpected task type:" + task);
                        continue;
                    }
                    runnable.init(context, task);
                    context.metaRepo.updateTaskNoReturn(task.getTaskId(), TaskStatus.RUNNING, null, null, null);
                    workerPool.execute(runnable);
                }
                
                
                long interval = context.conf.getLong(POLLING_INTERVAL_KEY, POLLING_INTERVAL_DEFAULT);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("going to sleep for " + interval + " milliseconds");
                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ex) {
                    LOG.info("someone waked me up! stopRequested=" + stopRequested);
                }
            }
        } catch (IOException ex) {
            LOG.error("unexpected exception in data task polling thread on Node-" + context.nodeId, ex);
        }
        stopped = true;
        LOG.info("stopped a data task polling thread on Node-" + context.nodeId);
    }
}
