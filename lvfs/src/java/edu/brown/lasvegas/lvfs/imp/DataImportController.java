package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.PartitionRawTextFilesTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * The class to control data import to LVFS.
 * <p>This class merely registers a job and a bunch of sub-tasks
 * to the metadata repository and waits for data nodes to
 * complete the actual work.</p>
 * 
 * <p>
 * For example, you can use this class as following:
 * <pre>
 * LVMetadataClient metaClient = new LVMetadataClient(new Configuration());
 * LVMetadataProtocol metaRepo = metaClient.getChannel ();
 * DataImportParameters param = new DataImportParameters(123);
 * param.getNodeFilePathMap().put(11, new String[]{"/home/user/test1.txt"});
 * param.getNodeFilePathMap().put(12, new String[]{"/home/user/test2.txt"});
 * new DataImportController(metaRepo, param).execute();
 * </pre>
 * </p>
 */
public class DataImportController {
    private static Logger LOG = Logger.getLogger(DataImportController.class);

    /**
     * Metadata repository.
     */
    private final LVMetadataProtocol metaRepo;
    
    private final DataImportParameters param;
    private final int jobId;

    public DataImportController (LVMetadataProtocol metaRepo, DataImportParameters param) throws IOException {
        this.metaRepo = metaRepo;
        this.param = param;
        this.jobId = metaRepo.createNewJobIdOnlyReturn("data import Fracture-" + param.getFractureId(), JobType.IMPORT_FRACTURE, null);
    }
    
    // differences between stopRequested and errorEncountered
    // errorEncountered: will cancel all the tasks, but will also wait to see all tasks are actually stopped.
    // stopRequested: will cancel all the tasks, but exit before checking they are actually stopped.
    private boolean stopRequested = false;
    private boolean errorEncountered = false;
    private String errorMessages = "";

    private boolean stopped = false;

    private final static long STOP_MAX_WAIT_MILLISECONDS = 3000L;

    /** forcibly cancel the data import and immediately stop this object. */
    public void stop () {
        stopRequested = true;
        try {
            metaRepo.updateJobNoReturn(jobId, JobStatus.CANCEL_REQUESTED, null, null);
        } catch (IOException ex) {
            LOG.error("failed to update job status", ex);
        }
        long initTime = System.currentTimeMillis();
        while (!stopped && System.currentTimeMillis() - initTime < STOP_MAX_WAIT_MILLISECONDS) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException ex) {
                // don't continue.. someone else might REALLY want to stop everything
                LOG.warn("gave up stopping", ex);
                break;
            }
            
        }
    }
    /**
     * Start a data import.
     * @return ID of the Job ({@link LVJob}) object created for this data import.
     */
    public int execute (DataImportParameters param) throws IOException {
        // First, create a job object for the import.
        LOG.info("importing Fracture-" + param.getFractureId());
        metaRepo.updateJobNoReturn(jobId, JobStatus.RUNNING, null, null);

        // partition raw files at each node
        if (!stopRequested && !errorEncountered) {
            partitionRawFiles ();
        }
        // collect and load the partitioned files into LVFS
        if (!stopRequested && !errorEncountered) {
            loadPartitionedFiles ();
        }
        
        stopped = true;
        if (errorEncountered) {
            metaRepo.updateJobNoReturn(jobId, JobStatus.ERROR, null, errorMessages);
        } else if (stopRequested) {
            metaRepo.updateJobNoReturn(jobId, JobStatus.CANCELED, null, null);
        } else {
            metaRepo.updateJobNoReturn(jobId, JobStatus.DONE, new DoubleWritable(1.0d), null);
        }
        LOG.info("exit the job.");
        return jobId;
    }
    
    private void partitionRawFiles () throws IOException {
        // let's create a local task for partitioning at each node
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : param.getNodeFilePathMap().keySet()) {

            // create parameter for the task
            PartitionRawTextFilesTaskParameters taskParam = new PartitionRawTextFilesTaskParameters();
            taskParam.setDateFormat(param.getDateFormat());
            taskParam.setDelimiter(param.getDelimiter());
            taskParam.setEncoding(param.getEncoding());
            taskParam.setFilePaths(param.getNodeFilePathMap().get(nodeId));
            taskParam.setFractureId(param.getFractureId());
            taskParam.setTimeFormat(param.getTimeFormat());
            taskParam.setTimestampFormat(param.getTimestampFormat());
            
            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.PARTITION_RAW_TEXT_FILES, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new local partitioning task: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        
        // then, let's wait until all tasks are done.
        // if any task threw an error, we stop the entire job (as this is a raw local drive, there is no back-up node to recover from a crash)
        
        int finishedCount = 0;
        while (!stopRequested && finishedCount != taskMap.size()) {
            try {
                Thread.sleep(errorEncountered ? 500 : 5000);
            } catch (InterruptedException ex) {
            }
            LOG.info("polling the progress of tasks...");
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
                    double jobProgress = 0.5d * finishedCount / taskMap.size();
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
    
    private void loadPartitionedFiles () throws IOException {
        // TODO implement
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
}
