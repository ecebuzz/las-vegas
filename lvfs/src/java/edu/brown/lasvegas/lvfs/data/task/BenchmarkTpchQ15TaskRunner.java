package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

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
 * of TPC-H Q15 Task.
 */
public abstract class BenchmarkTpchQ15TaskRunner extends DataTaskRunner<BenchmarkTpchQ15TaskParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ15TaskRunner.class);
    protected LVTable lineitem;
    protected LVColumn l_suppkey, l_extendedprice, l_discount, l_shipdate;
    protected long lowerShipdateValue, upperShipdateValue;

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
        
        this.l_suppkey = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_suppkey");
        assert (l_suppkey != null);
        this.l_extendedprice = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_extendedprice");
        assert (l_extendedprice != null);
        this.l_discount = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_discount");
        assert (l_discount != null);
        this.l_shipdate = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_shipdate");
        assert (l_shipdate != null);
        
        // convert the given input date to millisec from epoch time
        int date = parameters.getDate();
        int year = date / 10000;
        int month = (date % 10000) / 100;
        int day = date % 100;
        GregorianCalendar calendar = new GregorianCalendar(year, month + 1, day);
        lowerShipdateValue = calendar.getTimeInMillis();
        calendar.roll(Calendar.MONTH, 3); // + 3 months
        upperShipdateValue = calendar.getTimeInMillis();

        prepareInputsQ15();
    }
    protected abstract void prepareInputsQ15 () throws Exception;

    protected class LineitemFracture {
        /** read all tuples from the partition. */
        @SuppressWarnings("unchecked")
        LineitemFracture (LVReplicaPartition lineitemPartition) throws IOException {
            ColumnFileReaderBundle l_suppkeyReader = getReader(lineitemPartition, l_suppkey);
            ColumnFileReaderBundle l_extendedpriceReader = getReader(lineitemPartition, l_extendedprice);
            ColumnFileReaderBundle l_discountReader = getReader(lineitemPartition, l_discount);
            ColumnFileReaderBundle l_shipdateReader = getReader(lineitemPartition, l_shipdate);
            try {
                this.lineitemTuples = l_suppkeyReader.getFileBundle().getTupleCount();
                LOG.info("lineitem partition tuple count=" + lineitemTuples);
                assert (l_extendedpriceReader.getFileBundle().getTupleCount() == lineitemTuples);
                assert (l_discountReader.getFileBundle().getTupleCount() == lineitemTuples);
                
                LOG.info("reading l_suppkeyFile at once...");
                this.lsupps = new int[lineitemTuples];
                int readLSupps = ((TypedReader<Integer, int[]>) l_suppkeyReader.getDataReader()).readValues(lsupps, 0, lineitemTuples);
                LOG.info("read.");
                l_suppkeyReader.getDataReader().close();
                assert (readLSupps == lineitemTuples);

                LOG.info("reading l_extendedpriceFile at once...");
                this.prices = new double[lineitemTuples];
                int readPrice = ((TypedReader<Double, double[]>) l_extendedpriceReader.getDataReader()).readValues(prices, 0, lineitemTuples);
                LOG.info("read.");
                l_extendedpriceReader.getDataReader().close();
                assert (readPrice == lineitemTuples);
                
                LOG.info("reading l_discountFile at once...");
                this.discounts = new float[lineitemTuples];
                int readQuantities = ((TypedReader<Float, float[]>) l_discountReader.getDataReader()).readValues(discounts, 0, lineitemTuples);
                LOG.info("read.");
                l_discountReader.getDataReader().close();
                assert (readQuantities == lineitemTuples);

                LOG.info("reading l_shipdateFile at once...");
                this.shipdates = new long[lineitemTuples];
                int readShipdates = ((TypedReader<Long, long[]>) l_shipdateReader.getDataReader()).readValues(shipdates, 0, lineitemTuples);
                LOG.info("read.");
                l_shipdateReader.getDataReader().close();
                assert (readShipdates == lineitemTuples);
            } finally {
                l_suppkeyReader.close();
                l_extendedpriceReader.close();
                l_discountReader.close();
                l_shipdateReader.close();
            }
        }
        final int lineitemTuples;
        final int[] lsupps;
        final double[] prices;
        final float[] discounts;
        final long[] shipdates;
        // final Object shipdatesCompressed;  // TODO should be in-situ execution
    }
}
