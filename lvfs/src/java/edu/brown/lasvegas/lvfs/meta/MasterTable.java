package edu.brown.lasvegas.lvfs.meta;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * This table stores misc internal configurations/values.
 */
@Entity
public class MasterTable {
    @PrimaryKey
    public String key;
    
    public Object value;

    public static final String DBNAME = "LVFS_MASTER";
    public static final String EPOCH_SEQ = "EPOCH_SEQ";
}