package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;

/**
 * @see TaskType#BENCHMARK_TPCH_Q17
 */
public final class BenchmarkTpchQ17TaskRunner extends DataTaskRunner<BenchmarkTpchQ17TaskParameters> {
    private static Logger LOG = Logger.getLogger(BenchmarkTpchQ17TaskRunner.class);
    private LVTable lineitem, part;
    private LVColumn l_partkey, l_extendedprice, l_quantity, p_partkey, p_brand, p_container;
    /** num of partitions to be processed in this node. */
    private int partitionCount;
    private LVReplicaPartition lineitemPartitions[], partPartitions[];
    
    @Override
    protected String[] runDataTask() throws Exception {
        prepareInputs ();
        double totalSum = 0;
        for (int i = 0; i < partitionCount; ++i) {
            LOG.info("processing.. " + i + "/" + partitionCount);
            totalSum += processPartition (lineitemPartitions[i], partPartitions[i]);
        }
        // a hack to make it easy. this return value should be a file path, but let's just return the query result as the string.
        return new String[]{String.valueOf (totalSum)};
    }
    
    @SuppressWarnings("unchecked")
    private double processPartition (LVReplicaPartition lineitemPartition, LVReplicaPartition partPartition) throws IOException {
        ColumnFileReaderBundle p_partkeyReader = getReader(partPartition, p_partkey);
        ColumnFileReaderBundle p_brandReader = getReader(partPartition, p_brand);
        ColumnFileReaderBundle p_containerReader = getReader(partPartition, p_container);
        try {
            int partTuples = p_partkeyReader.getFileBundle().getTupleCount();
            LOG.info("part partition tuple count=" + partTuples);
            assert (p_brandReader.getFileBundle().getTupleCount() == partTuples);
            assert (p_containerReader.getFileBundle().getTupleCount() == partTuples);

            // first, check integer values that correspond to the given brand/container parameter.
            // if these are null (no corresponding value), then we even don't have to read lineitem. no result.
            Integer brandCompressed = ((OrderedDictionary<String, String[]>) p_brandReader.getDictionary()).compress(parameters.getBrand());
            assert (p_brandReader.getFileBundle().getDictionaryBytesPerEntry() == 1); // brand column has only 50 distinct values
            LOG.info("brand " + parameters.getBrand() + " corresponds to an integer value:" + brandCompressed);
            if (brandCompressed == null) {
                LOG.warn("brand " + parameters.getBrand() + " doesn't have corresponding entry in dictionary. no result.");
                return 0;
            }
            
            Integer containerCompressed = ((OrderedDictionary<String, String[]>) p_containerReader.getDictionary()).compress(parameters.getContainer());
            LOG.info("container " + parameters.getContainer() + " corresponds to an integer value:" + containerCompressed);
            if (containerCompressed == null) {
                LOG.warn("container " + parameters.getContainer() + " doesn't have corresponding entry in dictionary. no result.");
                return 0;
            }

            ColumnFileReaderBundle l_partkeyReader = getReader(lineitemPartition, l_partkey);
            ColumnFileReaderBundle l_extendedpriceReader = getReader(lineitemPartition, l_extendedprice);
            ColumnFileReaderBundle l_quantityReader = getReader(lineitemPartition, l_quantity);
            try {
                int lineitemTuples = l_partkeyReader.getFileBundle().getTupleCount();
                LOG.info("lineitem partition tuple count=" + lineitemTuples);
                assert (l_extendedpriceReader.getFileBundle().getTupleCount() == lineitemTuples);
                assert (l_quantityReader.getFileBundle().getTupleCount() == lineitemTuples);
                return processPartitionCore(
                    lineitemTuples, partTuples,
                    (TypedReader<Integer, int[]>) l_partkeyReader.getDataReader(),
                    (TypedReader<Double, double[]>) l_extendedpriceReader.getDataReader(),
                    (TypedReader<Float, float[]>) l_quantityReader.getDataReader(),
                    (TypedReader<Integer, int[]>) p_partkeyReader.getDataReader(),
                    (TypedReader<Byte, byte[]>) p_brandReader.getCompressedDataReader(), // without decompression
                    p_containerReader.getCompressedDataReader(), p_containerReader.getFileBundle().getDictionaryBytesPerEntry(), // without decompression
                    brandCompressed.byteValue(), containerCompressed.intValue());
            } finally {
                l_partkeyReader.close();
                l_extendedpriceReader.close();
                l_quantityReader.close();
            }
        } finally {
            p_partkeyReader.close();
            p_brandReader.close();
            p_containerReader.close();
        }
    }

    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private double processPartitionCore (
        final int lineitemTuples, final int partTuples,
        TypedReader<Integer, int[]> l_partkeyFile,
        TypedReader<Double, double[]> l_extendedpriceFile,
        TypedReader<Float, float[]> l_quantityFile,
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
        
        LOG.info("reading l_partkeyFile at once...");
        int[] lparts = new int[lineitemTuples];
        int readLParts = l_partkeyFile.readValues(lparts, 0, lineitemTuples);
        LOG.info("read.");
        l_partkeyFile.close();
        assert (readLParts == lineitemTuples);

        LOG.info("reading l_extendedpriceFile at once...");
        double[] prices = new double[lineitemTuples];
        int readPrice = l_extendedpriceFile.readValues(prices, 0, lineitemTuples);
        LOG.info("read.");
        l_extendedpriceFile.close();
        assert (readPrice == lineitemTuples);
        
        LOG.info("reading l_quantityFile at once...");
        float[] quantities = new float[lineitemTuples];
        int readQuantities = l_quantityFile.readValues(quantities, 0, lineitemTuples);
        LOG.info("read.");
        l_quantityFile.close();
        assert (readQuantities == lineitemTuples);
        
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

    private ColumnFileReaderBundle getReader (LVReplicaPartition partition, LVColumn column) throws IOException {
        assert (partition.getNodeId().intValue() == context.nodeId);
        LVColumnFile file = context.metaRepo.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), column.getColumnId());
        assert (file != null);
        ColumnFileBundle fileBundle = new ColumnFileBundle(file);
        return new ColumnFileReaderBundle(fileBundle, 0); // no buffering needed. we read them at once
    }

    private void prepareInputs () throws Exception {
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

        this.partitionCount = parameters.getLineitemPartitionIds().length;
        assert (partitionCount == parameters.getPartPartitionIds().length);
        assert (partitionCount > 0);
        this.lineitemPartitions = new LVReplicaPartition[partitionCount];
        this.partPartitions = new LVReplicaPartition[partitionCount];
        for (int i = 0; i < partitionCount; ++i) {
            lineitemPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getLineitemPartitionIds()[i]);
            partPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getPartPartitionIds()[i]);
        }
    }
}
