package edu.brown.lasvegas.lvfs.imp;

import java.util.List;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.util.ValueRange;

interface ValueSplitter {
    /** uniformly split the range into numSplits partitions. */
    List<ValueRange> split (ColumnType type, Comparable<?> min, Comparable<?> max, int numSplits);
}