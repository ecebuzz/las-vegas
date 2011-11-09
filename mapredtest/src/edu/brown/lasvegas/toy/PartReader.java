package edu.brown.lasvegas.toy;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.log4j.Logger;

public class PartReader {
    private static Logger LOG = Logger.getLogger(PartReader.class);
    public static void main(String[] args) throws Exception {
        LOG.info("started");
        byte[] buffer = new byte[64 << 20];
        Configuration conf = new Configuration();
        LOG.info("getting FS");
        
        DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.
            get(URI.create("hdfs://poseidon.smn.cs.brown.edu:9000/"), conf);
        Path path = new Path("hdfs://poseidon.smn.cs.brown.edu:9000/ssb/s4/part.tbl");
        LOG.info("opening file");
        DFSDataInputStream in = (DFSDataInputStream) hdfs.open(path);
        LOG.info("opened file");
        int readBytes;
        long stime = System.nanoTime();
        // compare time to read entire data vs read 10MB pos-12MB pos.
        // before running this experiment, execute cacheclear.sh (which calls drop_caches)
        {
            in.skipBytes(10 << 20);
            readBytes = in.read(buffer, 0, 2 << 20);
        }
        {
            // readBytes = in.read(buffer);
        }
        long etime = System.nanoTime();
        LOG.info("read " + readBytes + " bytes in " + ((etime - stime) / 1000000.0d) + " ms");
        LOG.info("curblock =" + in.getCurrentBlock().toString());
        LOG.info("closing..");
        in.close();
        LOG.info("closed..");
    }
}
