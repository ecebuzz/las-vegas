package edu.brown.lasvegas.toy;

import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

public class UploadTest {
    private static Logger LOG = Logger.getLogger(UploadTest.class);
    public static void main(String[] args) throws Exception {
        LOG.info("started");
        byte[] buffer = new byte[64 << 20];
        Configuration conf = new Configuration();
        LOG.info("getting FS");
        
        DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.
            get(URI.create("hdfs://poseidon.smn.cs.brown.edu:9000/"), conf);
        Path path = new Path("hdfs://poseidon.smn.cs.brown.edu:9000/toy/test.tbl");
        LOG.info("creating file");
        FSDataOutputStream out = hdfs.create(path, true, 1 << 20, (short) 1, 64 << 20);
        // FSDataOutputStream out = hdfs.create(path, true, 64 << 20, (short) 1, 64 << 20);
        LOG.info("created file");
        long stime = System.nanoTime();
        {
            for (int i = 0; i < 10; ++i) {
                LOG.info("writing " + i + "/10");
                Arrays.fill(buffer, (byte) (48 + i)); // number 0, 1, ...9
                out.write(buffer);
                out.hflush();
            }
        }
        long etime = System.nanoTime();
        LOG.info("wrote in " + ((etime - stime) / 1000000.0d) + " ms");
        out.close();
        LOG.info("closed..");
    }
}
