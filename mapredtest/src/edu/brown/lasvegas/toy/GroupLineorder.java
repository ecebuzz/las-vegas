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
 * Implements the following query on original (unsorted) Lineorder file. SELECT
 * lo_orderdate, SUM(lo_extendedprice) GROUP BY lo_orderdate;
 */
public class GroupLineorder {
    public static class LineReader extends Mapper<Object, Text, IntWritable, LongWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueStr = value.toString();
            String[] data = valueStr.split("\\|");
            int lo_orderdate = Integer.parseInt(data[5]);
            long lo_extendedprice = Long.parseLong(data[9]);
            context.write(new IntWritable(lo_orderdate), new LongWritable(lo_extendedprice));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "group lineorder");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(GroupLineorder.class);
        job.setMapperClass(LineReader.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(
                        // "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/lineorder/lineorder.tbl"
                        "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/lineorder.tbl"
                        ));
        FileOutputFormat.setOutputPath(job,
                        new Path("hdfs://poseidon.smn.cs.brown.edu:9000/tmp/out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
