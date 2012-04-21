package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.Map;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedReader;
import edu.brown.lasvegas.lvfs.data.RepartitionSummary;
import edu.brown.lasvegas.traits.ValueTraitsFactory;

/**
 * This one collects repartitioned files and then runs the query.
 * @see TaskType#BENCHMARK_TPCH_Q17_PLANB
 */
public final class BenchmarkTpchQ17PlanBTaskRunner extends BenchmarkTpchQ17TaskRunner {
	private Map<Integer, LVColumnFile[][]> repartitionedFiles;//key=nodeId

    @Override
    protected void prepareInputsQ17() throws Exception {
    	repartitionedFiles = RepartitionSummary.parseSummaryFiles(context, parameters.getRepartitionSummaryFileMap());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected double processPartition (int partition) throws IOException {
    	LVReplicaPartition partPartition = partPartitions[partition];
    	int partRange = partPartition.getRange();
    	
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

            Object[] mergedData = RepartitionSummary.mergeRepartitionedFilesOnMemory(
            		repartitionedFiles, context, partRange, new ColumnType[]{l_partkey.getType(), l_extendedprice.getType(), l_quantity.getType()}, 0);
        	if (mergedData == null) {
        		LOG.warn("no repartitioned files for this part partition:" + partPartition);
        		return 0;
        	}

            int lineitemTuples = ValueTraitsFactory.INTEGER_TRAITS.length((int[]) mergedData[0]);
            LOG.info("lineitem partition tuple count=" + lineitemTuples);
            assert (ValueTraitsFactory.DOUBLE_TRAITS.length((double[]) mergedData[1]) == lineitemTuples);
            assert (ValueTraitsFactory.FLOAT_TRAITS.length((float[]) mergedData[2]) == lineitemTuples);
            return processPartitionCore(
                lineitemTuples, partTuples,
                (int[]) mergedData[0], (double[]) mergedData[1], (float[]) mergedData[2],
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
}
