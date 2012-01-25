package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.JobController;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.placement.PlacementEventHandlerImpl;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * The job to import a new fracture.
 * <p>This class merely registers a job and a bunch of sub-tasks
 * to the metadata repository and waits for data nodes to
 * complete the actual work.</p>
 * 
 * <p>
 * For example, you can use this class as following:
 * <pre>
 * LVMetadataClient metaClient = new LVMetadataClient(new Configuration());
 * LVMetadataProtocol metaRepo = metaClient.getChannel ();
 * ImportFractureJobParameters param = new ImportFractureJobParameters(123);
 * param.getNodeFilePathMap().put(11, new String[]{"/home/user/test1.txt"});
 * param.getNodeFilePathMap().put(12, new String[]{"/home/user/test2.txt"});
 * new ImportFractureJobController(metaRepo).startSync(param);
 * </pre>
 * </p>
 * @see JobType#IMPORT_FRACTURE
 */
public class ImportFractureJobController implements JobController<ImportFractureJobParameters> {
    private static Logger LOG = Logger.getLogger(ImportFractureJobController.class);

    /**
     * Metadata repository.
     */
    private final LVMetadataProtocol metaRepo;
    
    private ImportFractureJobParameters param;
    private int jobId;
    private LVTable table;
    private LVFracture fracture;
    private LVReplicaGroup[] groups;
    private Map<Integer, LVReplicaScheme> defaultReplicaSchemes;
    private Map<Integer, LVReplicaScheme[]> otherReplicaSchemes;
    
    private final long stopMaxWaitMilliseconds;
    private final long taskJoinIntervalMilliseconds;
    private final long taskJoinIntervalOnErrorMilliseconds;

    public ImportFractureJobController (LVMetadataProtocol metaRepo) throws IOException {
        this (metaRepo, 3000L, 5000L, 500L);
    }
    public ImportFractureJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        this.metaRepo = metaRepo;
        this.stopMaxWaitMilliseconds = stopMaxWaitMilliseconds;
        this.taskJoinIntervalMilliseconds = taskJoinIntervalMilliseconds;
        this.taskJoinIntervalOnErrorMilliseconds = taskJoinIntervalOnErrorMilliseconds;
    }
    
    /** returns the newly created fracture. */
    public LVFracture getFracture () {
        return fracture;
    }
    
    // differences between stopRequested and errorEncountered
    // errorEncountered: will cancel all the tasks, but will also wait to see all tasks are actually stopped.
    // stopRequested: will cancel all the tasks, but exit before checking they are actually stopped.
    private boolean stopRequested = false;
    private boolean errorEncountered = false;
    private String errorMessages = "";

    private boolean stopped = false;
    @Override
    public boolean isStopped() {
        return stopped;
    }
    @Override
    public void requestStop() {
        stopRequested = true;
    }

    @Override
    public void stop () {
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
    public LVJob startAsync(ImportFractureJobParameters param) throws IOException {
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
    public LVJob startSync(ImportFractureJobParameters param) throws IOException {
        init (param);
        runInternal();
        return metaRepo.getJob(jobId);
    }
    
    private void init (ImportFractureJobParameters param) throws IOException {
        this.param = param;
        // create a new fracture.
        this.table = metaRepo.getTable(param.getTableId());
        assert (table != null);
        this.fracture = metaRepo.createNewFracture(table);
        assert (fracture != null);
        
        this.jobId = metaRepo.createNewJobIdOnlyReturn("data import Fracture-" + fracture.getFractureId(), JobType.IMPORT_FRACTURE, null);

        this.groups = metaRepo.getAllReplicaGroups(fracture.getTableId());
        this.defaultReplicaSchemes = new HashMap<Integer, LVReplicaScheme>();
        this.otherReplicaSchemes = new HashMap<Integer, LVReplicaScheme[]>();
        for (LVReplicaGroup group : groups) {
            LVReplicaScheme[] replicaSchemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (replicaSchemes.length > 0);
            // pick the one that will be loaded first. ideally a replica scheme without sorting.
            LVReplicaScheme defaultScheme = null;
            for (LVReplicaScheme scheme : replicaSchemes) {
                if (defaultScheme == null || scheme.getSortColumnId() == null) {
                    defaultScheme = scheme;
                }
                metaRepo.createNewReplica(scheme, fracture);
            }
            assert (defaultScheme != null);
            defaultReplicaSchemes.put(group.getGroupId(), defaultScheme);
            
            List<LVReplicaScheme> others = new ArrayList<LVReplicaScheme>();
            for (LVReplicaScheme scheme : replicaSchemes) {
                if (scheme != defaultScheme) {
                    others.add(scheme);
                }
            }
            otherReplicaSchemes.put(group.getGroupId(), others.toArray(new LVReplicaScheme[0]));
        }
        // assigns data nodes to each replica partition.
        new PlacementEventHandlerImpl(metaRepo).onNewFracture(fracture);
    }

    private void runInternal() {
        try {
            // First, create a job object for the import.
            LOG.info("importing Fracture-" + fracture.getFractureId());
            metaRepo.updateJobNoReturn(jobId, JobStatus.RUNNING, null, null);
    
            // partition raw files at each node
            TemporaryFilePath[] allPartitionedFiles = null;
            if (!stopRequested && !errorEncountered) {
                allPartitionedFiles = partitionRawFiles (0.0d, 1.0d / 3.0d); // 0%-33% progress
            }
    
            // collect and load the partitioned files into LVFS
            // here, we only load them to *one* replica scheme in each replica group.
            // other replica schemes are loaded after this task, using the buddy files.
            if (!stopRequested && !errorEncountered) {
                assert (allPartitionedFiles != null);
                loadPartitionedFiles (allPartitionedFiles, 1.0d / 3.0d, 2.0d / 3.0d); // 33%-66% progress
            }
            
            // loads other replica schemes in each replica group
            // this is supposed to be efficient because of the buddy files which are loaded in the previous tasks.
            if (!stopRequested && !errorEncountered) {
                copyFromBuddyFiles (2.0d / 3.0d, 1.0d); // 66%-100% progress
            }
    
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
    
    private TemporaryFilePath[] partitionRawFiles (double baseProgress, double completedProgress) throws IOException {
        // let's create a local task for partitioning at each node
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : param.getNodeFilePathMap().keySet()) {

            // create parameter for the task
            PartitionRawTextFilesTaskParameters taskParam = new PartitionRawTextFilesTaskParameters();
            taskParam.setDateFormat(param.getDateFormat());
            taskParam.setDelimiter(param.getDelimiter());
            taskParam.setEncoding(param.getEncoding());
            taskParam.setFilePaths(param.getNodeFilePathMap().get(nodeId));
            taskParam.setFractureId(fracture.getFractureId());
            taskParam.setTimeFormat(param.getTimeFormat());
            taskParam.setTimestampFormat(param.getTimestampFormat());
            taskParam.setTemporaryCompression(param.getTemporaryFileCompression());
            
            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.PARTITION_RAW_TEXT_FILES, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new local partitioning task: " + task);
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        
        joinTasks(taskMap, baseProgress, completedProgress);
        
        // extract resulting files
        if (!stopRequested && !errorEncountered) {
            List<TemporaryFilePath> allPartitionedFiles = new ArrayList<TemporaryFilePath> ();
            for (LVTask task : taskMap.values()) {
                assert (task.getStatus() == TaskStatus.DONE);
                for (String outputPath : task.getOutputFilePaths()) {
                    allPartitionedFiles.add(new TemporaryFilePath(outputPath));
                }
            }
            return allPartitionedFiles.toArray(new TemporaryFilePath[0]);
        } else {
            return null;
        }
    }

    private void loadPartitionedFiles (TemporaryFilePath[] allPartitionedFiles, double baseProgress, double completedProgress) throws IOException {
        // only loads default replica scheme in each replica group
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (LVReplicaGroup group : groups) {
            LVReplicaScheme defaultScheme = defaultReplicaSchemes.get(group.getGroupId());
            assert (defaultScheme != null);
            
            LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(defaultScheme.getSchemeId(), fracture.getFractureId());
            LVReplicaPartition[] partitions = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
            assert (partitions.length > 0);

            // key=partition (range)
            Map<Integer, List<TemporaryFilePath>> filesPerPartition = new HashMap<Integer, List<TemporaryFilePath>>();
            for (TemporaryFilePath partitionedFile : allPartitionedFiles) {
                if (partitionedFile.replicaGroupId == group.getGroupId()) {
                    List<TemporaryFilePath> list = filesPerPartition.get(partitionedFile.partition);
                    if (list == null) {
                        list = new ArrayList<TemporaryFilePath>();
                        filesPerPartition.put(partitionedFile.partition, list);
                    }
                    list.add (partitionedFile);
                }
            }

            // to which node are these assigned?
            // key=assigned node ID (NOT the ID of the node the file currently resides)
            Map<Integer, NodeFileLoadAssignment> assignmentsPerNode = new HashMap<Integer, NodeFileLoadAssignment>();
            for (LVReplicaPartition partition : partitions) {
                Integer nodeId = partition.getNodeId();
                if (nodeId == null) {
                    throw new IOException ("this partition has not been assigned to data node. " + partition);
                }
                if (!filesPerPartition.containsKey(partition.getRange())) {
                    // this means there was no tuple to import for the partition.
                    // set the empty status and ignore.
                    metaRepo.updateReplicaPartitionNoReturn(partition.getPartitionId(), ReplicaPartitionStatus.EMPTY, null);
                    continue;
                }
                NodeFileLoadAssignment assignments = assignmentsPerNode.get(nodeId);
                if (assignments == null) {
                    assignments = new NodeFileLoadAssignment();
                    assignmentsPerNode.put(nodeId, assignments);
                }
                assignments.files.addAll(filesPerPartition.get(partition.getRange()));
                assignments.partitions.add(partition);
            }
            
            // for each node, start a new task
            assert (!assignmentsPerNode.isEmpty());
            for (Integer nodeId : assignmentsPerNode.keySet()) {
                NodeFileLoadAssignment assignments = assignmentsPerNode.get(nodeId);
                LoadPartitionedTextFilesTaskParameters taskParam = new LoadPartitionedTextFilesTaskParameters();
                taskParam.setDateFormat(param.getDateFormat());
                taskParam.setDelimiter(param.getDelimiter());
                taskParam.setEncoding(param.getEncoding());
                taskParam.setFractureId(fracture.getFractureId());
                taskParam.setTimeFormat(param.getTimeFormat());
                taskParam.setTimestampFormat(param.getTimestampFormat());
                taskParam.setReplicaId(replica.getReplicaId());
                taskParam.setReplicaPartitionIds(assignments.getReplicaPartitionIds());
                taskParam.setTemporaryPartitionedFiles(assignments.getFilePaths());
                taskParam.setTemporaryCompression(param.getTemporaryFileCompression());
                
                int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.LOAD_PARTITIONED_TEXT_FILES, taskParam.writeToBytes());
                LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
                LOG.info("launched new local loading task: " + task);
                assert (!taskMap.containsKey(taskId));
                taskMap.put(taskId, task);
            }
        }

        joinTasks(taskMap, baseProgress, completedProgress);
    }
    private static class NodeFileLoadAssignment {
        List<TemporaryFilePath> files = new ArrayList<TemporaryFilePath>();
        List<LVReplicaPartition> partitions = new ArrayList<LVReplicaPartition>();
        int[] getReplicaPartitionIds () {
            int[] ids = new int[partitions.size()];
            for (int i = 0; i < ids.length; ++i) {
                ids [i] = partitions.get(i).getPartitionId();
            }
            return ids;
        }
        String[] getFilePaths () {
            String[] paths = new String[files.size()];
            for (int i = 0; i < files.size(); ++i) {
                paths [i] = files.get(i).getFilePath();
            }
            return paths;
        }
    }

    private void copyFromBuddyFiles (double baseProgress, double completedProgress) throws IOException {
        // loads other replica schemes from the buddy-files in default scheme.
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (LVReplicaGroup group : groups) {
            LVReplicaScheme defaultScheme = defaultReplicaSchemes.get(group.getGroupId());
            assert (defaultScheme != null);
            
            LVReplicaScheme[] otherSchemes = otherReplicaSchemes.get(group.getGroupId());
            if (otherSchemes.length == 0) {
                continue;
            }
            for (LVReplicaScheme scheme : otherSchemes) {
                LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(scheme.getSchemeId(), fracture.getFractureId());
                LVReplica buddyReplica = metaRepo.getReplicaFromSchemeAndFracture(defaultScheme.getSchemeId(), fracture.getFractureId());
                LVReplicaPartition[] partitions = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
                // key = node id
                Map<Integer, NodeFileLoadAssignment> assignmentsPerNode = new HashMap<Integer, NodeFileLoadAssignment>();

                for (LVReplicaPartition partition : partitions) {
                    Integer nodeId = partition.getNodeId();
                    if (nodeId == null) {
                        throw new IOException ("this partition has not been assigned to data node. " + partition);
                    }
                    LVReplicaPartition buddyPartition = metaRepo.getReplicaPartitionByReplicaAndRange(buddyReplica.getReplicaId(), partition.getRange());
                    if (buddyPartition.getStatus() == ReplicaPartitionStatus.EMPTY) {
                        // this partition has no tuples. so, just set the EMPTY status and ignore.
                        metaRepo.updateReplicaPartitionNoReturn(partition.getPartitionId(), ReplicaPartitionStatus.EMPTY, null);
                        continue;
                    }
                    assert (buddyPartition.getStatus() == ReplicaPartitionStatus.OK);
                    NodeFileLoadAssignment assignments = assignmentsPerNode.get(nodeId);
                    // List<LVReplicaPartition> assignments = partitionsPerAssignedNode.get(nodeId);
                    if (assignments == null) {
                        assignments = new NodeFileLoadAssignment();
                        assignmentsPerNode.put(nodeId, assignments);
                    }
                    assignments.partitions.add(partition);
                }

                // for each node, start a new task
                for (Integer nodeId : assignmentsPerNode.keySet()) {
                    NodeFileLoadAssignment assignments = assignmentsPerNode.get(nodeId);
                    RecoverPartitionFromBuddyTaskParameters taskParam = new RecoverPartitionFromBuddyTaskParameters();
                    taskParam.setPartitionIds(assignments.getReplicaPartitionIds());
                    taskParam.setReplicaId(replica.getReplicaId());
                    taskParam.setBuddyReplicaId(buddyReplica.getReplicaId());
                    
                    int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.RECOVER_PARTITION_FROM_BUDDY, taskParam.writeToBytes());
                    LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
                    LOG.info("launched new task to construct replica from buddy files: " + task);
                    assert (!taskMap.containsKey(taskId));
                    taskMap.put(taskId, task);
                }
            }
        }
        joinTasks(taskMap, baseProgress, completedProgress);
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
    private void joinTasks (SortedMap<Integer, LVTask> taskMap, double baseProgress, double completedProgress) throws IOException {
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
