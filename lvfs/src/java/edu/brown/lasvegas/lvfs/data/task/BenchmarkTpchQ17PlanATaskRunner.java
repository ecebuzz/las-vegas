package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;

/**
 * @see TaskType#BENCHMARK_TPCH_Q17_PLANA
 */
public final class BenchmarkTpchQ17PlanATaskRunner extends BenchmarkTpchQ17TaskRunner {
    /** key=partition range, value=corresponding lineitem partitions (one for each fracture). */
    private HashMap<Integer, ArrayList<LVReplicaPartition>> lineitemPartitions;
    @Override
    protected void prepareInputsQ17() throws Exception {
        int[] lineitemPartitionIds = parameters.getLineitemPartitionIds();
        this.lineitemPartitions = new HashMap<Integer, ArrayList<LVReplicaPartition>>();
        for (int i = 0; i < lineitemPartitionIds.length; ++i) {
            LVReplicaPartition partition = context.metaRepo.getReplicaPartition(lineitemPartitionIds[i]);
            int range = partition.getRange();
            ArrayList<LVReplicaPartition> partitions = lineitemPartitions.get(range);
            if (partitions == null) {
                partitions = new ArrayList<LVReplicaPartition>();
                lineitemPartitions.put(range, partitions);
            }
            partitions.add(partition);
        }
    }

    @SuppressWarnings("unchecked")
    protected double processPartition (int partition) throws IOException {
    	LVReplicaPartition partPartition = partPartitions[partition];
        ArrayList<LVReplicaPartition> lineitemPartitionList = lineitemPartitions.get(partPartition.getRange()); // one for each fracture
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
            
            // part table has only one fracutre, but lineitem table might have multiple partitions.
            // we merge the fractures first (on memory. as each fracture is partkey sorted, this is very fast)
            ArrayList<LineitemFracture> lineitemFractures = new ArrayList<LineitemFracture>();
            for (LVReplicaPartition lineitemPartition : lineitemPartitionList) {
                lineitemFractures.add(new LineitemFracture(lineitemPartition));
            }
            LineitemFracture mergedFracture = new LineitemFracture(lineitemFractures.toArray(new LineitemFracture[lineitemFractures.size()]));

            return processPartitionCore(
                mergedFracture.lineitemTuples, partTuples,
                mergedFracture.lparts, mergedFracture.prices, mergedFracture.quantities,
                (TypedReader<Integer, int[]>) p_partkeyReader.getDataReader(),
                (TypedReader<Byte, byte[]>) p_brandReader.getCompressedDataReader(), // without decompression
                p_containerReader.getCompressedDataReader(), p_containerReader.getFileBundle().getDictionaryBytesPerEntry(), // without decompression
                brandCompressed.byteValue(), containerCompressed.intValue());
        } finally {
            p_partkeyReader.close();
            p_brandReader.close();
            p_containerReader.close();
        }
    }

    private class LineitemFracture {
        /** read all tuples from the partition. */
        @SuppressWarnings("unchecked")
        LineitemFracture (LVReplicaPartition lineitemPartition) throws IOException {
            ColumnFileReaderBundle l_partkeyReader = getReader(lineitemPartition, l_partkey);
            ColumnFileReaderBundle l_extendedpriceReader = getReader(lineitemPartition, l_extendedprice);
            ColumnFileReaderBundle l_quantityReader = getReader(lineitemPartition, l_quantity);
            try {
                this.lineitemTuples = l_partkeyReader.getFileBundle().getTupleCount();
                LOG.info("lineitem partition tuple count=" + lineitemTuples);
                assert (l_extendedpriceReader.getFileBundle().getTupleCount() == lineitemTuples);
                assert (l_quantityReader.getFileBundle().getTupleCount() == lineitemTuples);

                
                LOG.info("reading l_partkeyFile at once...");
                this.lparts = new int[lineitemTuples];
                int readLParts = ((TypedReader<Integer, int[]>) l_partkeyReader.getDataReader()).readValues(lparts, 0, lineitemTuples);
                LOG.info("read.");
                l_partkeyReader.getDataReader().close();
                assert (readLParts == lineitemTuples);

                LOG.info("reading l_extendedpriceFile at once...");
                this.prices = new double[lineitemTuples];
                int readPrice = ((TypedReader<Double, double[]>) l_extendedpriceReader.getDataReader()).readValues(prices, 0, lineitemTuples);
                LOG.info("read.");
                l_extendedpriceReader.getDataReader().close();
                assert (readPrice == lineitemTuples);
                
                LOG.info("reading l_quantityFile at once...");
                this.quantities = new float[lineitemTuples];
                int readQuantities = ((TypedReader<Float, float[]>) l_quantityReader.getDataReader()).readValues(quantities, 0, lineitemTuples);
                LOG.info("read.");
                l_quantityReader.getDataReader().close();
                assert (readQuantities == lineitemTuples);
            } finally {
                l_partkeyReader.close();
                l_extendedpriceReader.close();
                l_quantityReader.close();
            }
        }
        /** the constructor to merge fractures. */
        LineitemFracture (LineitemFracture[] fractures) {
            if (fractures.length == 1) {
                LOG.info("single lineitem fracture. no need to merge");
                LineitemFracture fracture = fractures[0];
                this.lineitemTuples = fracture.lineitemTuples;
                this.lparts = fracture.lparts;
                this.prices = fracture.prices;
                this.quantities = fracture.quantities;
            } else {
                LOG.info("merging " + (fractures.length) + " lineitem fractures...");
                long start = System.currentTimeMillis();

                int totalLineitemTuples = 0;
                for (LineitemFracture fracture : fractures) {
                    totalLineitemTuples += fracture.lineitemTuples;
                }
                this.lineitemTuples = totalLineitemTuples;
                this.lparts = new int[lineitemTuples];
                this.prices = new double[lineitemTuples];
                this.quantities = new float[lineitemTuples];

                // simply merge assuming sortedness.
                // if #fractures is really large, loser's-tree might reduce the CPU overhead,
                // but this merging is anyway fast...
                final int fractureLen = fractures.length;
                int mergedPosition = 0;
                int[] positions = new int[fractureLen];
                Arrays.fill(positions, 0);
                while (true) {
                    int min = Integer.MAX_VALUE;
                    // first, find the min
                    for (int i = 0; i < fractureLen; ++i) {
                        if (positions[i] == fractures[i].lineitemTuples) {
                            continue;
                        }
                        if (fractures[i].lparts[positions[i]] < min) {
                            min = fractures[i].lparts[positions[i]];
                        }
                    }
                    if (min == Integer.MAX_VALUE) {
                        break; // done
                    }
                    
                    // then, push all tuples with the min value to the merged array
                    for (int i = 0; i < fractureLen; ++i) {
                        assert (positions[i] >= fractures[i].lineitemTuples || fractures[i].lparts[positions[i]] >= min); // otherwise partkey-sortedness is broken
                        while (positions[i] < fractures[i].lineitemTuples && fractures[i].lparts[positions[i]] == min) {
                            lparts[mergedPosition] = min;
                            prices[mergedPosition] = fractures[i].prices[positions[i]];
                            quantities[mergedPosition] = fractures[i].quantities[positions[i]];
                            ++mergedPosition;
                            ++positions[i];
                        }
                    }
                }
                assert (mergedPosition == lineitemTuples);
                
                long end = System.currentTimeMillis();
                LOG.info("merged " + (fractures.length) + " lineitem fractures (" + lineitemTuples + " tuples) in " + (end - start) + "ms");
            }
        }
        final int lineitemTuples;
        final int[] lparts;
        final double[] prices;
        final float[] quantities;
    }
}
