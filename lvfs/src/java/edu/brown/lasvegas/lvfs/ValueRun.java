package edu.brown.lasvegas.lvfs;

/**
 * Represents an entry in Run-Length-Encoding.
 * This is usually an internal object and callers will see
 * de-compressed values only.
 * However, some query optimization will need in-situ execution
 * where it directly deals with this object. 
 */
public final class ValueRun<T> {
    /** the tuple (in the column file) where this run starts at. */
    public int startTuple;
    /** number of tuples this value runs for. */
    public int runLength;
    /** the value shared among all tuples in the run. */
    public T value;
    
    public ValueRun () {
        startTuple = 0;
        runLength = 0;
        value = null;
    }
    
    public ValueRun (int startTuple, int runLength, T value) {
        this.startTuple = startTuple;
        this.runLength = runLength;
        this.value = value;
    }
    
    @Override
    public String toString() {
        return "[startTuple=" + startTuple + ",runLength=" + runLength + ",value=" + value + "]";
    }
}
