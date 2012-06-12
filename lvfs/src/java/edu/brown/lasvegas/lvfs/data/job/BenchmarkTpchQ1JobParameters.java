package edu.brown.lasvegas.lvfs.data.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.brown.lasvegas.JobParameters;

/**
 * Query parameters for TPCH Q1.
 */
public final class BenchmarkTpchQ1JobParameters extends JobParameters {
    /** ID of lineitem table. */
    private int tableId;

    /** ID of LVReplicaScheme to use. */
    private int schemeId;
    
    /** query parameter: [DELTA]. should be between 60 and 120. */
    private int deltaDays;

    /**
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        tableId = in.readInt();
        schemeId = in.readInt();
        deltaDays = in.readInt();
    }
    
    /**
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(tableId);
    	out.writeInt(schemeId);
    	out.writeInt(deltaDays);
    }

    /**
     * Gets the iD of lineitem table.
     *
     * @return the iD of lineitem table
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Sets the iD of lineitem table.
     *
     * @param tableId the new iD of lineitem table
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the iD of LVReplicaScheme to use.
     *
     * @return the iD of LVReplicaScheme to use
     */
    public int getSchemeId() {
        return schemeId;
    }

    /**
     * Sets the iD of LVReplicaScheme to use.
     *
     * @param schemeId the new iD of LVReplicaScheme to use
     */
    public void setSchemeId(int schemeId) {
        this.schemeId = schemeId;
    }

    /**
     * Gets the query parameter: [DELTA].
     *
     * @return the query parameter: [DELTA]
     */
    public int getDeltaDays() {
        return deltaDays;
    }

    /**
     * Sets the query parameter: [DELTA].
     *
     * @param deltaDays the new query parameter: [DELTA]
     */
    public void setDeltaDays(int deltaDays) {
        this.deltaDays = deltaDays;
    }
}
