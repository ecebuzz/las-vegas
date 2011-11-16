package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

import edu.brown.lasvegas.LVTableReader;
import edu.brown.lasvegas.util.ValueRange;

/**
 * LVFS API for client programs.
 * Analogous to {@link FileSystem}, but a few semantics are different
 * to provide faster accesses.
 */
public class LasVegasFileSystem extends Configured {


    /**
     * The only method to add new data.
     * @param tableId ID of the table to insert
     * @param newRange key range of base group's partitioning column.
     * can be NULL if the base group has no explicit partitioning column.
     * In that case, the implicit "epoch" is used as partitioning column
     * and the range is automatically generated.
     * @param datasource provides the data to import.
     * @return ID of the newly created fracture
    */
    public int importFracture (int tableId, ValueRange<?> newRange,
      LVTableReader datasource) throws IOException {
        return -1;
    }

    /**
     * Merge or split fractures.
     * @param mergedFractureIds Fractures to be merged. Must be adjacent in terms of
     * partitioning.
     * @param countAfterMerging if you are merging, specify 1. if
     * re-balancing, 2 or more.
    */
    public void rebalanceFracture (Set<Integer> mergedFractureIds, int countAfterMerging) throws IOException {
        
    }
}
