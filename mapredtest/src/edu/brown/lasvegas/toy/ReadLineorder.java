package edu.brown.lasvegas.toy;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Simplified SSB Q1.3 on the original lineorder.tbl. The answer should be
 * 12110966276 or something on scale 1 (depends on random seed for dbgen).
 */
public class ReadLineorder {

    public static class LineReader extends Mapper<Object, Text, IntWritable, LongWritable> {
        private IntWritable label = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\|");
            int lo_orderdate = Integer.parseInt(data[5]);
            assert (lo_orderdate >= 19920101);
            assert (lo_orderdate < 20000101);
            if (lo_orderdate >= 19940210 && lo_orderdate <= 19940216) {
                int lo_discount = Integer.parseInt(data[11]);
                assert (lo_discount >= 0);
                assert (lo_discount < 100);
                if (lo_discount >= 5 && lo_discount <= 7) {
                    int lo_quantity = Integer.parseInt(data[8]);
                    assert (lo_quantity >= 0);
                    if (lo_quantity >= 26 && lo_quantity <= 30) {
                        long lo_extendedprice = Long.parseLong(data[9]);
                        assert (lo_extendedprice >= 0);
                        context.write(label, new LongWritable(lo_extendedprice * lo_discount));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "read lineorder");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(ReadLineorder.class);
        job.setMapperClass(LineReader.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(
                        "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/lineorder.tbl"
                        // "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/lineorder/lineorder.tbl"
                        ));
        FileOutputFormat.setOutputPath(job,
                        new Path("hdfs://poseidon.smn.cs.brown.edu:9000/tmp/out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
