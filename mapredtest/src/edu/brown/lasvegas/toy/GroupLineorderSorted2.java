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
import org.apache.log4j.Logger;

/**
 * Additionally does Combine by itself.
 */
public class GroupLineorderSorted2 {
    private static Logger LOG = Logger.getLogger(GroupLineorderSorted2.class);

    public static class LineReader extends Mapper<Object, Text, IntWritable, LongWritable> {
        int cur_odate;
        long cur_sum;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueStr = value.toString();
            if (valueStr.startsWith("#")) {
                LOG.warn("seems like a header line??:" + valueStr);
                return;
            }
            String[] data = valueStr.split("\\|");
            int lo_orderdate = Integer.parseInt(data[5]);
            long lo_extendedprice = Long.parseLong(data[9]);
            if (cur_odate == lo_orderdate) {
                cur_sum += lo_extendedprice;
            } else {
                context.write(new IntWritable(cur_odate), new LongWritable(cur_sum));
                cur_odate = lo_orderdate;
                cur_sum = lo_extendedprice;
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            cur_odate = 0;
            cur_sum = 0;
            if (context.nextKeyValue()) {
                String header = context.getCurrentValue().toString();
                if (header.startsWith("#")) {
                    LOG.info("found a header: " + header);
                } else {
                    LOG.warn("no header in this block?? first line=" + header);
                }
            } else {
                LOG.warn("no lines in this block??");
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (cur_odate != 0) {
                context.write(new IntWritable(cur_odate), new LongWritable(cur_sum));
                cur_odate = 0;
                cur_sum = 0;
            }
        }

        public void run(Context context) throws IOException, InterruptedException {
            LOG.info("reading a block...");
            setup(context);
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
            cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "group lineorder sorted2");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(GroupLineorderSorted2.class);
        job.setMapperClass(LineReader.class);
        // job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(
                        "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/lineorder_od.tbl"
                        // "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/lineorder_od.tbl"
                        ));
        FileOutputFormat.setOutputPath(job,
                        new Path("hdfs://poseidon.smn.cs.brown.edu:9000/tmp/out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
