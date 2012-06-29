package edu.brown.lasvegas.server;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * A short program to compact {@link LVJob} and {@link LVTask} in the metadata repository.
 * For more details, see {@link LVMetadataProtocol#compactJobAndTask(boolean, boolean, boolean, long)}.
 */
public class CompactJobAndTask {
    /** this is public just for testcase. */
    public static void execute (LVMetadataProtocol metaRepo, boolean compactOnly, boolean taskOnly, boolean finishedOnly, long minimalAgeMilliseconds) throws IOException {
        System.out.println ("calling compactJobAndTask()..");
        metaRepo.compactJobAndTask(compactOnly, taskOnly, finishedOnly, minimalAgeMilliseconds);
        System.out.println ("done.");
    }
    
    public static void main (String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("usage: java " + CompactJobAndTask.class.getName() + " <metadata repository address> <compactOnly:false/true> <taskOnly:false/true> <finishedOnly:false/true> <minimalAgeMilliseconds:0->");
            System.err.println("ex: java " + CompactJobAndTask.class.getName() + " poseidon:28710 false false false 0");
            return;
        }
        String metaRepoAddress = args[0];
        Configuration conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metaRepoAddress);
        LVMetadataClient client = new LVMetadataClient(conf);
        try {
            LVMetadataProtocol metaRepo = client.getChannel();
            execute(metaRepo, Boolean.parseBoolean(args[1]), Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Long.parseLong(args[4]));
        } finally {
            client.release();
        }
    }
}
