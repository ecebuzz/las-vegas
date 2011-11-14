package edu.brown.lasvegas.toy;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Do lineorder-part join. This one utilizes pre-sorted
 * lineorder and indexed part tables.
 * select sum(p_size*lo_supplycost) from lineorder
 * join part on (lineorder.lo_partkey=part.p_partkey)
 */
public class PartSortedJoin {
    private static Logger LOG = Logger.getLogger(PartSortedJoin.class);

    public static class LineReader extends Mapper<Object, Text, IntWritable, LongWritable> {
        long cur_sum;
        int minPartKey;
        int maxPartKey;
        PartContent parts;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueStr = value.toString();
            if (valueStr.startsWith("#")) {
                LOG.warn("seems like a header line??:" + valueStr);
                return;
            }
            String[] data = valueStr.split("\\|");
            int lo_partkey = Integer.parseInt(data[3]);
            assert (lo_partkey >= minPartKey);
            assert (lo_partkey <= maxPartKey);
            int lo_supplycost = Integer.parseInt(data[13]);
            Integer p_size = parts.keySizeMap.get(lo_partkey);
            if (p_size == null) {
                LOG.error("part " + lo_partkey + " not found");
            }
            assert (p_size != null);
            cur_sum += p_size * lo_supplycost;
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            cur_sum = 0;
            if (context.nextKeyValue()) {
                String header = context.getCurrentValue().toString();
                if (header.startsWith("#")) {
                    LOG.info("found a header: " + header);
                    String[] data = header.substring(1).split("\\|");
                    minPartKey = Integer.parseInt(data[1]);
                    maxPartKey = Integer.parseInt(data[2]);
                    parts = new PartContent (minPartKey, maxPartKey);
                } else {
                    LOG.warn("no header in this block?? first line=" + header);
                }
            } else {
                LOG.warn("no lines in this block??");
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(1), new LongWritable(cur_sum));
            cur_sum = 0;
        }
/* default is fine
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
            cleanup(context);
        }*/
    }

    /**
     * a part of part table.
     */
    static class PartContent {
        HashMap<Integer, Integer> keySizeMap = new HashMap<Integer, Integer>(); //partkey->p_size map
        PartContent (int minPartKey, int maxPartKey) throws IOException {
            IndexFileContent indexFile = new IndexFileContent(
                "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/part_pk.tbl.idx");
            long startPos = indexFile.getStartPosition(minPartKey);
            LOG.info("reading Part table from " + minPartKey + " to "
                            + maxPartKey + ", starting from " + startPos + "th byte");
            Configuration conf = new Configuration();
            String partTableUri = "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/part_pk.tbl";
            FileSystem hdfs = DistributedFileSystem.get(URI.create(partTableUri), conf);
            DataInputStream in = hdfs.open(new Path(partTableUri));
            in.skip(startPos);
            BufferedReader reader = new BufferedReader (new InputStreamReader(in, "UTF-8"), 1 << 20);
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String[] data = line.split("\\|");
                int partKey = Integer.parseInt(data[0]);
                if (partKey < minPartKey) {
                    continue; // this is possible because the index is sparse
                }
                if (partKey > maxPartKey) {
                    break;
                }
                int partSize = Integer.parseInt(data[7]);
                keySizeMap.put(partKey, partSize);
            }
            reader.close();
            LOG.info("read " + keySizeMap.size() + " parts");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "lineorder part join sorted");
        job.setInputFormatClass(LVTextInputFormat.class);
        job.setJarByClass(PartSortedJoin.class);
        job.setMapperClass(LineReader.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(
            "hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/lineorder_partk.tbl"
        ));
        FileOutputFormat.setOutputPath(job,
                        new Path("hdfs://poseidon.smn.cs.brown.edu:9000/tmp/out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
