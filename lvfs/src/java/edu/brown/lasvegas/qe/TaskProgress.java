package edu.brown.lasvegas.qe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Progress and/or results of a query execution task.
 */
public final class TaskProgress implements Writable {
    /** ID of the query execution task. */
    private int taskId;
    /** Whether the task is finished (whether successfully or not). */
    private boolean finished;
    /** Whether there happened any critical errors. */
    private boolean failed;
    /** total count of phases in the task. */
    private int phaseCount;
    /** count of completed phases in the task. */
    private int completedPhaseCount;
    /** The path of HDFS file containing query results. Only available the task successfully finished. */
    private String resultFilePath;

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "TaskProgress: taskId=" + taskId + ", finished=" + finished + ", failed=" + failed
        + ", phaseCount=" + phaseCount + ", completedPhaseCount=" + completedPhaseCount + ",resultFilePath=" + resultFilePath;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(taskId);
        out.writeInt(phaseCount);
        out.writeInt(completedPhaseCount);
        out.writeBoolean(finished);
        out.writeBoolean(failed);
        out.writeBoolean(resultFilePath == null);
        if (resultFilePath != null) {
            out.writeUTF(resultFilePath);
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        taskId = in.readInt();
        phaseCount = in.readInt();
        completedPhaseCount = in.readInt();
        finished = in.readBoolean();
        failed = in.readBoolean();
        boolean isNull = in.readBoolean();
        if (isNull) {
            resultFilePath = null;
        } else {
            resultFilePath = in.readUTF();
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static TaskProgress read (DataInput in) throws IOException {
        TaskProgress obj = new TaskProgress();
        obj.readFields(in);
        return obj;
    }

// auto-generated getters/setters (comments by JAutodoc)    
    
    /**
     * Checks if is whether the task is finished (whether successfully or not).
     *
     * @return the whether the task is finished (whether successfully or not)
     */
    public boolean isFinished() {
        return finished;
    }
    
    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    /**
     * Sets the whether the task is finished (whether successfully or not).
     *
     * @param finished the new whether the task is finished (whether successfully or not)
     */
    public void setFinished(boolean finished) {
        this.finished = finished;
    }
    
    /**
     * Checks if is whether there happened any critical errors.
     *
     * @return the whether there happened any critical errors
     */
    public boolean isFailed() {
        return failed;
    }
    
    /**
     * Sets the whether there happened any critical errors.
     *
     * @param failed the new whether there happened any critical errors
     */
    public void setFailed(boolean failed) {
        this.failed = failed;
    }
    
    /**
     * Gets the total count of phases in the task.
     *
     * @return the total count of phases in the task
     */
    public int getPhaseCount() {
        return phaseCount;
    }
    
    /**
     * Sets the total count of phases in the task.
     *
     * @param phaseCount the new total count of phases in the task
     */
    public void setPhaseCount(int phaseCount) {
        this.phaseCount = phaseCount;
    }
    
    /**
     * Gets the count of completed phases in the task.
     *
     * @return the count of completed phases in the task
     */
    public int getCompletedPhaseCount() {
        return completedPhaseCount;
    }
    
    /**
     * Sets the count of completed phases in the task.
     *
     * @param completedPhaseCount the new count of completed phases in the task
     */
    public void setCompletedPhaseCount(int completedPhaseCount) {
        this.completedPhaseCount = completedPhaseCount;
    }
    
    /**
     * Gets the path of HDFS file containing query results.
     *
     * @return the path of HDFS file containing query results
     */
    public String getResultFilePath() {
        return resultFilePath;
    }
    
    /**
     * Sets the path of HDFS file containing query results.
     *
     * @param resultFilePath the new path of HDFS file containing query results
     */
    public void setResultFilePath(String resultFilePath) {
        this.resultFilePath = resultFilePath;
    }
}
