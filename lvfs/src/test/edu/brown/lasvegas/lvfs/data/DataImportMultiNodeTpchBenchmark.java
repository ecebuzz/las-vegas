package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.client.LVMetadataClient;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.server.LVCentralNode;

/**
 * TPCH version.
 */
public class DataImportMultiNodeTpchBenchmark extends DataImportTpchBenchmark {
    private static final Logger LOG = Logger.getLogger(DataImportMultiNodeTpchBenchmark.class);

    private LVMetadataClient client;

    public DataImportMultiNodeTpchBenchmark(LVMetadataProtocol metaRepo, int partitionCount, int factTableFractures,
                    LVMetadataClient client) {
        super(metaRepo, partitionCount, factTableFractures);
        this.client = client;
    }
    public static DataImportMultiNodeTpchBenchmark getInstance (String metadataAddress, int partitionCount, int factTableFractures) throws IOException {
        Configuration conf = new Configuration();
        conf.set(LVCentralNode.METAREPO_ADDRESS_KEY, metadataAddress);
        LVMetadataClient client = new LVMetadataClient(conf);
        LOG.info("connected to metadata repository: " + metadataAddress);
        LVMetadataProtocol metaRepo = client.getChannel();
        return new DataImportMultiNodeTpchBenchmark(metaRepo, partitionCount, factTableFractures, client);
    }
    protected void tearDown () throws IOException {
        if (client != null) {
            client.release();
            client = null;
        }
    }
    public static void main (String[] args) throws Exception {
        LOG.info("running a multi node experiment..");
        if (args.length < 6) {
            System.err.println("usage: java " + DataImportMultiNodeTpchBenchmark.class.getName() + " <partitionCount> <metadata repository address> <name of the file that lists input files for lineitem table> <name of the file that lists input files for part table> <name of the file that lists input files for customer table> <name of the file that lists input files for orders table> (<#fractures of fact tables>:default is 1)");
            System.err.println("ex: java " + DataImportMultiNodeTpchBenchmark.class.getName() + " 2 poseidon:28710 inputs_lineitem.txt inputs_part.txt inputs_customer.txt inputs_orders.txt 2");
            // It should be a partitioned tbl (see TPCH's dbgen manual. eg: ./dbgen -T L -s 4 -S 1 -C 2; ./dbgen -T P -s 4 -S 1 -C 2 ).
            return;
        }
        int partitionCount = Integer.parseInt(args[0]);
        if (partitionCount <= 0) {
            throw new IllegalArgumentException ("invalid partition count :" + args[0]);
        }
        LOG.info("partitionCount=" + partitionCount);
        String metaRepoAddress = args[1];
        LOG.info("metaRepoAddress=" + metaRepoAddress);
        String lineitemInputFileName = args[2];
        String partInputFileName = args[3];
        String customerInputFileName = args[4];
        String ordersInputFileName = args[5];
        int factTableFractures = 1;
        if (args.length >= 7) {
            factTableFractures = Integer.parseInt(args[6]);
            if (factTableFractures < 1) {
                throw new IllegalArgumentException ("invalid fractures count:" + args[6]);
            }
        }
        
        DataImportMultiNodeTpchBenchmark program = DataImportMultiNodeTpchBenchmark.getInstance(metaRepoAddress, partitionCount, factTableFractures);
        program.setUp();
        program.exec(lineitemInputFileName, partInputFileName, customerInputFileName, ordersInputFileName);
    }
}
