package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Represents a small task to be processed in one node.
 * Task is always a part of a bigger and global Job ({@link LVJob}).
 */
@Entity
public final class LVTask implements LVObject {
    
    public static final String IX_JOB_ID = "IX_JOB_ID";
    /**
     * ID of the global job this local task belongs to.
     */
    @SecondaryKey(name=IX_JOB_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVJob.class)
    private int jobId;

    public static final String IX_NODE_ID = "IX_NODE_ID";
    /**
     * ID of the node {@link LVRackNode} this local task runs on.
     */
    @SecondaryKey(name=IX_NODE_ID, relate=Relationship.MANY_TO_ONE, relatedEntity=LVRackNode.class)
    private int nodeId;

    /**
     * A unique (system-wide) ID of this local task.
     */
    @PrimaryKey
    private int taskId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return taskId;
    }

    /** Type of this local task. */
    private TaskType type = TaskType.INVALID;
    
    /** Current status of this task. */
    private TaskStatus status = TaskStatus.INVALID;
    
    /** fraction of the work done (finished=1.0). just for report purpose. */
    private double progress;
    
    /** The time this task has been created (largely the time this task has been started). */
    private Date startedTime = new Date(0L);
    
    /** The time this task has finished. */
    private Date finishedTime = new Date(0L);
    
    /** the _local_ paths (relative to the local HDFS's root dir) of this taks's result files. */
    private String[] outputFilePaths = new String[0];
    
    /** the dumped message of the error (if the task finished with an error).*/
    private String errorMessages = "";
    
    /**
     * @see edu.brown.lasvegas.LVObject#getObjectType()
     */
    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.TASK;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Task-" + taskId + "(jobId=" + jobId + ")"
        + " type=" + type + ", progress=" + (progress * 100.0d) + "%"
        + ", status=" + status
        + ", nodeId=" + nodeId
        + ", startedTime=" + startedTime + ", finishedTime=" + finishedTime
        + ", outputFilePaths=" + outputFilePaths
        + ", errorMessages=" + errorMessages;
    }

    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(taskId);
        out.writeInt(jobId);
        out.writeInt(nodeId);
        out.writeInt(type == null ? TaskType.INVALID.ordinal() : type.ordinal());
        out.writeInt(status == null ? TaskStatus.INVALID.ordinal() : status.ordinal());
        out.writeDouble(progress);
        out.writeLong(startedTime == null ? 0L : startedTime.getTime());
        out.writeLong(finishedTime == null ? 0L : finishedTime.getTime());
        out.writeUTF(errorMessages == null ? "" : errorMessages);
        if (outputFilePaths == null) {
            out.writeInt(0);
        } else {
            out.writeInt(outputFilePaths.length);
            for (String path : outputFilePaths) {
                out.writeUTF(path == null ? "" : path);
            }
        }
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        taskId = in.readInt();
        jobId = in.readInt();
        nodeId = in.readInt();
        type = TaskType.values()[in.readInt()];
        status = TaskStatus.values()[in.readInt()];
        progress = in.readDouble();
        if (startedTime == null) {
            startedTime = new Date(in.readLong());
        } else {
            startedTime.setTime(in.readLong());
        }
        if (finishedTime == null) {
            finishedTime = new Date(in.readLong());
        } else {
            finishedTime.setTime(in.readLong());
        }
        errorMessages = in.readUTF();
        outputFilePaths = new String[in.readInt()];
        for (int i = 0; i < outputFilePaths.length; ++i) {
            outputFilePaths[i] = in.readUTF();
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVTask read (DataInput in) throws IOException {
        LVTask obj = new LVTask();
        obj.readFields(in);
        return obj;
    }

 // auto-generated getters/setters (comments by JAutodoc)    

    /**
      * Gets the iD of the global job this local task belongs to.
      *
      * @return the iD of the global job this local task belongs to
      */
     public int getJobId() {
        return jobId;
    }


    /**
     * Sets the iD of the global job this local task belongs to.
     *
     * @param jobId the new iD of the global job this local task belongs to
     */
    public void setJobId(int jobId) {
        this.jobId = jobId;
    }


    /**
     * Gets the a unique (system-wide) ID of this local task.
     *
     * @return the a unique (system-wide) ID of this local task
     */
    public int getTaskId() {
        return taskId;
    }


    /**
     * Sets the a unique (system-wide) ID of this local task.
     *
     * @param taskId the new a unique (system-wide) ID of this local task
     */
    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }


    /**
     * Gets the type of this local task.
     *
     * @return the type of this local task
     */
    public TaskType getType() {
        return type;
    }


    /**
     * Sets the type of this local task.
     *
     * @param type the new type of this local task
     */
    public void setType(TaskType type) {
        this.type = type;
    }


    /**
     * Gets the current status of this task.
     *
     * @return the current status of this task
     */
    public TaskStatus getStatus() {
        return status;
    }


    /**
     * Sets the current status of this task.
     *
     * @param status the new current status of this task
     */
    public void setStatus(TaskStatus status) {
        this.status = status;
    }



    /**
     * Gets the time this task has been created (largely the time this task has been started).
     *
     * @return the time this task has been created (largely the time this task has been started)
     */
    public Date getStartedTime() {
        return startedTime;
    }

    /**
     * Sets the time this task has been created (largely the time this task has been started).
     *
     * @param startedTime the new time this task has been created (largely the time this task has been started)
     */
    public void setStartedTime(Date startedTime) {
        this.startedTime = startedTime;
    }

    /**
     * Gets the time this task has finished.
     *
     * @return the time this task has finished
     */
    public Date getFinishedTime() {
        return finishedTime;
    }


    /**
     * Sets the time this task has finished.
     *
     * @param finishedTime the new time this task has finished
     */
    public void setFinishedTime(Date finishedTime) {
        this.finishedTime = finishedTime;
    }


    /**
     * Gets the dumped message of the error (if the task finished with an error).
     *
     * @return the dumped message of the error (if the task finished with an error)
     */
    public String getErrorMessages() {
        return errorMessages;
    }


    /**
     * Sets the dumped message of the error (if the task finished with an error).
     *
     * @param errorMessages the new dumped message of the error (if the task finished with an error)
     */
    public void setErrorMessages(String errorMessages) {
        this.errorMessages = errorMessages;
    }

    /**
     * Gets the iD of the node {@link LVRackNode} this local task runs on.
     *
     * @return the iD of the node {@link LVRackNode} this local task runs on
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * Sets the iD of the node {@link LVRackNode} this local task runs on.
     *
     * @param nodeId the new iD of the node {@link LVRackNode} this local task runs on
     */
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Gets the fraction of the work done (finished=1.
     *
     * @return the fraction of the work done (finished=1
     */
    public double getProgress() {
        return progress;
    }

    /**
     * Sets the fraction of the work done (finished=1.
     *
     * @param progress the new fraction of the work done (finished=1
     */
    public void setProgress(double progress) {
        this.progress = progress;
    }

    /**
     * Gets the _local_ paths (relative to the local HDFS's root dir) of this taks's result files.
     *
     * @return the _local_ paths (relative to the local HDFS's root dir) of this taks's result files
     */
    public String[] getOutputFilePaths() {
        return outputFilePaths;
    }

    /**
     * Sets the _local_ paths (relative to the local HDFS's root dir) of this taks's result files.
     *
     * @param outputFilePaths the new _local_ paths (relative to the local HDFS's root dir) of this taks's result files
     */
    public void setOutputFilePaths(String[] outputFilePaths) {
        this.outputFilePaths = outputFilePaths;
    }
}
