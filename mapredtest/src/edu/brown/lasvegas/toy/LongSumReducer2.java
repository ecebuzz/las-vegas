package edu.brown.lasvegas.toy;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/** just to check if combiner is really called. same as {@link LongSumReducer} otherwise. */
public class LongSumReducer2 extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    private static Logger LOG = Logger.getLogger(LongSumReducer2.class);
    private LongWritable result = new LongWritable();
    boolean everCalled = false;
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        if (!everCalled) {
            LOG.warn("uuuuuuuuuuuush  Combiner is called!!!");
            everCalled = true;
        }
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}