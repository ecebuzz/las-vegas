package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;

/**
 * @see TaskType#BENCHMARK_TPCH_Q17_PLANA
 */
public final class BenchmarkTpchQ17PlanATaskRunner extends BenchmarkTpchQ17TaskRunner {
    private LVReplicaPartition lineitemPartitions[];
    @Override
    protected void prepareInputsQ17() throws Exception {
        assert (partPartitionCount == parameters.getLineitemPartitionIds().length);
        this.lineitemPartitions = new LVReplicaPartition[partPartitionCount];
        for (int i = 0; i < partPartitionCount; ++i) {
            lineitemPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getLineitemPartitionIds()[i]);
        }
    }

    @SuppressWarnings("unchecked")
    protected double processPartition (int partition) throws IOException {
    	LVReplicaPartition lineitemPartition = lineitemPartitions[partition];
    	LVReplicaPartition partPartition = partPartitions[partition];
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

                
                LOG.info("reading l_partkeyFile at once...");
                int[] lparts = new int[lineitemTuples];
                int readLParts = ((TypedReader<Integer, int[]>) l_partkeyReader.getDataReader()).readValues(lparts, 0, lineitemTuples);
                LOG.info("read.");
                l_partkeyReader.getDataReader().close();
                assert (readLParts == lineitemTuples);

                LOG.info("reading l_extendedpriceFile at once...");
                double[] prices = new double[lineitemTuples];
                int readPrice = ((TypedReader<Double, double[]>) l_extendedpriceReader.getDataReader()).readValues(prices, 0, lineitemTuples);
                LOG.info("read.");
                l_extendedpriceReader.getDataReader().close();
                assert (readPrice == lineitemTuples);
                
                LOG.info("reading l_quantityFile at once...");
                float[] quantities = new float[lineitemTuples];
                int readQuantities = ((TypedReader<Float, float[]>) l_quantityReader.getDataReader()).readValues(quantities, 0, lineitemTuples);
                LOG.info("read.");
                l_quantityReader.getDataReader().close();
                assert (readQuantities == lineitemTuples);

                return processPartitionCore(
                    lineitemTuples, partTuples,
                    lparts, prices, quantities,
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
}
