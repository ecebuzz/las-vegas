package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.data.task.DiskCacheFlushTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Flushes disk cache in data nodes.
 */
public class DiskCacheFlushJobController extends AbstractJobController<DiskCacheFlushJobParameters> {
    private static Logger LOG = Logger.getLogger(DiskCacheFlushJobController.class);

    public DiskCacheFlushJobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public DiskCacheFlushJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    @Override
	protected void initDerived() throws IOException {
        this.jobId = metaRepo.createNewJobIdOnlyReturn("disk cache flush", JobType.DISK_CACHE_FLUSH, null);
	}
	@Override
	protected void runDerived() throws IOException {
        SortedMap<Integer, LVTask> taskMap = new TreeMap<Integer, LVTask>();
        for (Integer nodeId : param.getNodeFilePathMap().keySet()) {

            // create parameter for the task
        	DiskCacheFlushTaskParameters taskParam = new DiskCacheFlushTaskParameters();
            taskParam.setPath(param.getNodeFilePathMap().get(nodeId));
            taskParam.setUseDropCaches(param.isUseDropCaches());
            
            int taskId = metaRepo.createNewTaskIdOnlyReturn(jobId, nodeId, TaskType.DISK_CACHE_FLUSH, taskParam.writeToBytes());
            LVTask task = metaRepo.updateTask(taskId, TaskStatus.START_REQUESTED, null, null, null);
            LOG.info("launched new disk cache flushing task: " + task + ". file to read=" + taskParam.getPath() + ". useDropCaches=" + taskParam.isUseDropCaches());
            assert (!taskMap.containsKey(taskId));
            taskMap.put(taskId, task);
        }
        
        joinTasks(taskMap, 0, 1);
	}
}
