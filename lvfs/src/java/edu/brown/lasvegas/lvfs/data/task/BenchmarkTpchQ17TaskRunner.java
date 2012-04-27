package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;

/**
 * Base class for the two implementations (fast query plan and slower query plan)
 * of TPC-H Q17 Task.
 */
public abstract class BenchmarkTpchQ17TaskRunner extends DataTaskRunner<BenchmarkTpchQ17TaskParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ17TaskRunner.class);
    protected LVTable lineitem, part;
    protected LVColumn l_partkey, l_extendedprice, l_quantity, p_partkey, p_brand, p_container;
    /** num of partitions to be processed in this node. */
    protected int partPartitionCount;
    protected LVReplicaPartition partPartitions[];
    
    @Override
    protected final String[] runDataTask() throws Exception {
        prepareInputs ();
        double totalSum = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < partPartitionCount; ++i) {
            LOG.info("processing.. " + i + "/" + partPartitionCount);
            totalSum += processPartition (i);
        }
        long endTime = System.currentTimeMillis();
        LOG.info("total processPartition() time: " + (endTime - startTime) + "ms");
        // a hack to make it easy. this return value should be a file path, but let's just return the query result as the string.
        return new String[]{String.valueOf (totalSum)};
    }
    
    protected abstract double processPartition (int partPartition) throws IOException;
    
    
    protected final ColumnFileReaderBundle getReader (LVReplicaPartition partition, LVColumn column) throws IOException {
        assert (partition.getNodeId().intValue() == context.nodeId);
        LVColumnFile file = context.metaRepo.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), column.getColumnId());
        assert (file != null);
        ColumnFileBundle fileBundle = new ColumnFileBundle(file);
        return new ColumnFileReaderBundle(fileBundle, 0); // no buffering needed. we read them at once
    }

    protected final void prepareInputs () throws Exception {
        this.lineitem = context.metaRepo.getTable(parameters.getLineitemTableId());
        assert (lineitem != null);
        this.part = context.metaRepo.getTable(parameters.getPartTableId());
        assert (part != null);
        
        this.l_partkey = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_partkey");
        assert (l_partkey != null);
        this.l_extendedprice = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_extendedprice");
        assert (l_extendedprice != null);
        this.l_quantity = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_quantity");
        assert (l_quantity != null);

        this.p_partkey = context.metaRepo.getColumnByName(part.getTableId(), "p_partkey");
        assert (p_partkey != null);
        this.p_brand = context.metaRepo.getColumnByName(part.getTableId(), "p_brand");
        assert (p_brand != null);
        this.p_container = context.metaRepo.getColumnByName(part.getTableId(), "p_container");
        assert (p_container != null);

        this.partPartitionCount = parameters.getPartPartitionIds().length;
        assert (partPartitionCount > 0);
        this.partPartitions = new LVReplicaPartition[partPartitionCount];
        for (int i = 0; i < partPartitionCount; ++i) {
            partPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getPartPartitionIds()[i]);
        }
        prepareInputsQ17();
    }
    protected abstract void prepareInputsQ17 () throws Exception;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected final double processPartitionCore (
        final int lineitemTuples, final int partTuples,
        int[] lparts, double[] prices, float[] quantities,
        TypedReader<Integer, int[]> p_partkeyFile,
        TypedReader<Byte, byte[]> p_brandFile,
        TypedReader p_containerFile, byte containerBytesPerEntry, // only container has variable types. byte/short/int depending on the size of dataset
        final byte brandCompressed, final int containerCompressed) throws IOException {

        LOG.info("reading p_brandFile at once...");
        byte[] brands = new byte[partTuples];
        int readBrand = p_brandFile.readValues(brands, 0, partTuples);
        LOG.info("read.");
        p_brandFile.close();
        assert (readBrand == partTuples);

        LOG.info("reading p_partkeyFile at once...");
        int[] pparts = new int[partTuples];
        int readPPart = p_partkeyFile.readValues(pparts, 0, partTuples);
        LOG.info("read.");
        p_partkeyFile.close();
        assert (readPPart == partTuples);

        LOG.info("reading p_containerFile at once...");
        assert (containerBytesPerEntry == 1 || containerBytesPerEntry == 2 || containerBytesPerEntry == 4);
        Object containers = p_containerFile.getValueTraits().createArray(partTuples);
        int containerRead = p_containerFile.readValues(containers, 0, partTuples);
        LOG.info("read.");
        p_containerFile.close();
        assert (containerRead == partTuples);
        
        int matchedPartKeyCount = 0;
        int matchedLineitemCount = 0;
        double sum = 0;
        int lpartPos = 0;
        for (int partIndex = 0; partIndex < partTuples; ++partIndex) {
            // filter by brand and container
            if (brands[partIndex] != brandCompressed) {
                continue;
            }
            if (containerBytesPerEntry == 1) {
                if (((byte[]) containers)[partIndex] != containerCompressed) {
                    continue;
                }
            } else if (containerBytesPerEntry == 2) {
                if (((short[]) containers)[partIndex] != containerCompressed) {
                    continue;
                }
            } else {
                assert (containerBytesPerEntry == 4);
                if (((int[]) containers)[partIndex] != containerCompressed) {
                    continue;
                }
            }
            
            int partkey = pparts[partIndex];
            ++matchedPartKeyCount;
            if (LOG.isInfoEnabled() && matchedPartKeyCount % 100 == 0) {
                LOG.info(matchedPartKeyCount + "th matching partkey=" + partkey + ", partIndex=" + partIndex + ", lpartPos=" + lpartPos + ". current sum=" + sum);
            }
            
            // find the corresponding position in lineitem
            for (; lpartPos < lineitemTuples && lparts[lpartPos] < partkey; ++lpartPos);
            if (lpartPos == lineitemTuples || lparts[lpartPos] > partkey) {
                continue;
            }
            
            // for each partkey, first aggregate over lineitem to get the average quantity of the partkey
            assert (lparts[lpartPos] == partkey);
            double subQuantityTotal = 0;
            int lpartEnd;
            for (lpartEnd = lpartPos; lpartEnd < lineitemTuples && lparts[lpartEnd] == partkey; ++lpartEnd) {
                subQuantityTotal += quantities[lpartEnd];
            }
            double thresholdQuantity = 0.2d * subQuantityTotal / (lpartEnd - lpartPos);
            
            // then, check if the quantity is below the threahold 
            for (int i = lpartPos; i < lpartEnd; ++ i) {
                if (quantities[i] < thresholdQuantity) {
                    sum += prices[i] / 7.0d;
                    ++matchedLineitemCount;
                }
            }
            lpartPos = lpartEnd;
        }
        
        LOG.info("read the partition. in total " + matchedPartKeyCount + " matching partkey and " + matchedLineitemCount + " matching lineitem tuples. sum=" + sum);
        return sum;
    }
}
