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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Similar to ReadLineorder, but this uses a sorted input file and skips input
 * to speed it up. The input file is prepared by {@link SortTblFile}. It has a
 * header line at the beginning of each 64MB block. Header line is
 * "#SORT-COL|min_key|max_key|count" eg, "#5|19920101|19920928|674575".
 */
public class ReadLineorderSkip {
    private static Logger LOG = Logger.getLogger(ReadLineorderSkip.class);

    public static class LineReader extends Mapper<Object, Text, IntWritable, LongWritable> {
        private IntWritable label = new IntWritable(1);
        /** whether the map function told that we can exit the loop now. */
        private boolean reachedEnd;
        //private int sortCol;
        private int minKey;
        private int maxKey;
        //private int totalCountInThisBlock;
        
        private long sum;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueStr = value.toString();
            if (valueStr.startsWith("#")) {
                LOG.warn("seems like a header line??:" + valueStr);
                return;
            }
            String[] data = valueStr.split("\\|");
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
                        sum += lo_extendedprice * lo_discount;
                    }
                }
            } else {
                if (lo_orderdate > 19940216) {
                    // as the input file is sorted by orderdate, we can stop
                    // here.
                    reachedEnd = true;
                    LOG.info("reached end of interesting lines");
                }
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            reachedEnd = false;
            sum = 0;
            if (context.getCurrentKey() != null) {
                LOG.info("??? line=" + context.getCurrentValue().toString());
            }
            if (context.nextKeyValue()) {
                String header = context.getCurrentValue().toString();
                if (header.startsWith("#")) {
                    LOG.info("found a header: " + header);
                    String[] data = header.substring(1).split("\\|");
                    //sortCol = Integer.parseInt(data[0]);
                    minKey = Integer.parseInt(data[1]);
                    maxKey = Integer.parseInt(data[2]);
                    //totalCountInThisBlock = Integer.parseInt(data[3]);
                } else {
                    LOG.warn("no header in this block?? first line=" + header);
                }
            } else {
                LOG.warn("no lines in this block??");
                reachedEnd = true;
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(label, new LongWritable(sum));
        }
 
        public void run(Context context) throws IOException, InterruptedException {
            LOG.info("reading a block...");
            setup(context);
            if (maxKey < 19940210 || minKey > 19940216) {
                LOG.info("this block definitely doesn't have satisfying lines. skipped");
                reachedEnd = true;
            }
            while (!reachedEnd && context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
            cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", "\r\n");
        Job job = Job.getInstance(conf, "read lineorder skip");
        job.setInputFormatClass(LVTextInputFormat.class);
        job.setJarByClass(ReadLineorderSkip.class);
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
