package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

import edu.brown.lasvegas.InputTableReader;

/**
 * LVFS API for client programs.
 * Analogous to {@link FileSystem}, but a few semantics are different
 * to provide faster accesses.
 */
public class LasVegasFileSystem extends Configured {


    /**
     * The only method to add new data.
     * @param tableId ID of the table to insert
     * @param datasource provides the data to import.
     * @return ID of the newly created fracture
    */
    public int importFracture (int tableId, InputTableReader datasource) throws IOException {
        return -1;
    }

    /**
     * Merge or split fractures.
     * @param mergedFractureIds Fractures to be merged. Must be adjacent in terms of
     * fracturing.
     * @param countAfterMerging if you are merging, specify 1. if
     * re-balancing (or splitting), 2 or more.
    */
    public void rebalanceFractures (Set<Integer> mergedFractureIds, int countAfterMerging) throws IOException {
        
    }
}

