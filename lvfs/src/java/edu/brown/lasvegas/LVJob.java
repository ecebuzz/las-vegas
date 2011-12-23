package edu.brown.lasvegas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Represents a job to process in Las-Vegas, such as a data import and a query.
 * Job consists of several tasks on each node, which .
 * Differences between Job and Task are, 1) Job is global while Task is on one node,
 * 2) Job has Tasks as children while Task has no child Task.
 */
@Entity
public final class LVJob implements LVObject {
    /**
     * A unique (system-wide) ID of this job.
     */
    @PrimaryKey
    private int jobId;
    
    /**
     * @see edu.brown.lasvegas.LVObject#getPrimaryKey()
     */
    @Override
    public int getPrimaryKey() {
        return jobId;
    }
    
    /** Type of this job (such as data import and querying). */
    private JobType type = JobType.INVALID;
    
    /** Current status of this job. */
    private JobStatus status = JobStatus.INVALID;

    /** A short user-given description of this job. */
    private String description = "";
    
    /** fraction of the work done (finished=1.0). just for report purpose. */
    private double progress;
    
    /** The time this job has been created (largely the time this job has been started). */
    private Date startedTime = new Date(0L);
    
    /** The time this job has finished. */
    private Date finishedTime = new Date(0L);
    
    /** the dumped message of the error (if the job finished with an error).*/
    private String errorMessages = "";
    
    /** serialized job parameters.*/
    private byte[] parameters;

    @Override
    public LVObjectType getObjectType() {
        return LVObjectType.JOB;
    }
    
    
    @Override
    public String toString() {
        return "Job-" + jobId + " type=" + type + ", progress=" + (progress * 100.0d) + "%"
        + ", status=" + status + ", description=" + description
        + ", startedTime=" + startedTime + ", finishedTime=" + finishedTime
        + ", errorMessages=" + errorMessages;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobId);
        out.writeInt(type == null ? JobType.INVALID.ordinal() : type.ordinal());
        out.writeInt(status == null ? JobStatus.INVALID.ordinal() : status.ordinal());
        out.writeDouble(progress);
        out.writeLong(startedTime == null ? 0L : startedTime.getTime());
        out.writeLong(finishedTime == null ? 0L : finishedTime.getTime());
        out.writeUTF(description == null ? "" : description);
        out.writeUTF(errorMessages == null ? "" : errorMessages);
        if (parameters == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(parameters.length);
            out.write(parameters);
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        jobId = in.readInt();
        type = JobType.values()[in.readInt()];
        status = JobStatus.values()[in.readInt()];
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
        description = in.readUTF();
        errorMessages = in.readUTF();
        {
            int len = in.readInt();
            if (len < 0) {
                parameters = null;
            } else {
                parameters = new byte[len];
                in.readFully(parameters);
            }
        }
    }
    /** Creates and returns a new instance of this class from the data input.*/
    public static LVJob read (DataInput in) throws IOException {
        LVJob obj = new LVJob();
        obj.readFields(in);
        return obj;
    }

// auto-generated getters/setters (comments by JAutodoc)    
    /**
     * Gets the a unique (system-wide) ID of this job.
     *
     * @return the a unique (system-wide) ID of this job
     */
    public int getJobId() {
        return jobId;
    }

    /**
     * Sets the a unique (system-wide) ID of this job.
     *
     * @param jobId the new a unique (system-wide) ID of this job
     */
    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    /**
     * Gets the type of this job (such as data import and querying).
     *
     * @return the type of this job (such as data import and querying)
     */
    public JobType getType() {
        return type;
    }

    /**
     * Sets the type of this job (such as data import and querying).
     *
     * @param type the new type of this job (such as data import and querying)
     */
    public void setType(JobType type) {
        this.type = type;
    }

    /**
     * Gets the current status of this job.
     *
     * @return the current status of this job
     */
    public JobStatus getStatus() {
        return status;
    }

    /**
     * Sets the current status of this job.
     *
     * @param status the new current status of this job
     */
    public void setStatus(JobStatus status) {
        this.status = status;
    }

    /**
     * Gets the a short user-given description of this job.
     *
     * @return the a short user-given description of this job
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the a short user-given description of this job.
     *
     * @param description the new a short user-given description of this job
     */
    public void setDescription(String description) {
        this.description = description;
    }

    

    /**
     * Gets the time this job has been created (largely the time this job has been started).
     *
     * @return the time this job has been created (largely the time this job has been started)
     */
    public Date getStartedTime() {
        return startedTime;
    }


    /**
     * Sets the time this job has been created (largely the time this job has been started).
     *
     * @param startedTime the new time this job has been created (largely the time this job has been started)
     */
    public void setStartedTime(Date startedTime) {
        this.startedTime = startedTime;
    }


    /**
     * Gets the time this job has finished.
     *
     * @return the time this job has finished
     */
    public Date getFinishedTime() {
        return finishedTime;
    }

    /**
     * Sets the time this job has finished.
     *
     * @param finishedTime the new time this job has finished
     */
    public void setFinishedTime(Date finishedTime) {
        this.finishedTime = finishedTime;
    }

    /**
     * Gets the dumped message of the error (if the job finished with an error).
     *
     * @return the dumped message of the error (if the job finished with an error)
     */
    public String getErrorMessages() {
        return errorMessages;
    }

    /**
     * Sets the dumped message of the error (if the job finished with an error).
     *
     * @param errorMessages the new dumped message of the error (if the job finished with an error)
     */
    public void setErrorMessages(String errorMessages) {
        this.errorMessages = errorMessages;
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
     * Gets the serialized job parameters.
     *
     * @return the serialized job parameters
     */
    public byte[] getParameters() {
        return parameters;
    }


    /**
     * Sets the serialized job parameters.
     *
     * @param parameters the new serialized job parameters
     */
    public void setParameters(byte[] parameters) {
        this.parameters = parameters;
    }
    
}
