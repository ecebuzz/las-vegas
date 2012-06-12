package edu.brown.lasvegas.lvfs.data.task;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;
import edu.brown.lasvegas.lvfs.data.job.BenchmarkTpchQ1JobController.Q1ResultSet;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.tuple.ColumnFileTupleReader;
import edu.brown.lasvegas.tuple.TupleBuffer;

/**
 * TPC-H Q1 implementation.
 * The query is so simple.
 * So this implementation works for arbitrary partitioning, sorting, and number of fractures. 
 */
public class BenchmarkTpchQ1TaskRunner extends DataTaskRunner<BenchmarkTpchQ1TaskParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ1TaskRunner.class);

    private LVTable table;
    private static final String[] columnNames = new String[]{
        "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate"};
    private LVColumn[] columns;
    private LVReplicaPartition[] partitions;
    private long thresholdShipdate;
    
    private Q1ResultSet result;
    private TupleBuffer buffer;
    @Override
    protected final String[] runDataTask() throws Exception {
        prepareInputs ();
        result = new Q1ResultSet();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < partitions.length; ++i) {
            LOG.info("processing.. " + i + "/" + partitions.length);
            processPartition (i);
        }
        long endTime = System.currentTimeMillis();
        LOG.info("total processPartition() time: " + (endTime - startTime) + "ms");
        LOG.info("sub-result:" + result);
        
        // serialize the sub-ranking to a file
        VirtualFile tmpFolder = new LocalVirtualFile(context.localLvfsTmpDir);
        if (!tmpFolder.exists()) {
        	tmpFolder.mkdirs();
        }
        assert (tmpFolder.exists());
        String filename = "tmp_q1_subresult_" + Math.abs(new Random(System.nanoTime()).nextInt());
        VirtualFile subresultFile = tmpFolder.getChildFile(filename);
        VirtualFileOutputStream out = subresultFile.getOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        result.write(dataOut);
        dataOut.flush();
        dataOut.close();
        LOG.info("wrote sub-result to " + subresultFile.getAbsolutePath());
        return new String[]{subresultFile.getAbsolutePath()};
    }
    
    private void processPartition (int partitionIndex) throws IOException {
        LVReplicaPartition partition = partitions[partitionIndex];
        
        
        ColumnFileBundle[] columnFiles = new ColumnFileBundle[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            LVColumnFile file = context.metaRepo.getColumnFileByReplicaPartitionAndColumn(partition.getPartitionId(), columns[i].getColumnId());
            if (file == null) {
                throw new IOException ("no column file in " + partition + " for column " + columns[i]);
            }
            columnFiles[i] = new ColumnFileBundle(file);
        }

        long allCount = 0, droppedCount = 0;
        ColumnFileTupleReader readers = new ColumnFileTupleReader(columnFiles, 1 << 22); // quite big buffer
        while (true) {
            buffer.resetCount();
            int read = readers.nextBatch(buffer);
            if (read < 0) {
                break;
            }
            String[] returnflag = buffer.getColumnBufferAsString(0);
            String[] linestatus = buffer.getColumnBufferAsString(1);
            float[] quantity = buffer.getColumnBufferAsFloat(2);
            double[] price = buffer.getColumnBufferAsDouble(3);
            float[] discount = buffer.getColumnBufferAsFloat(4);
            float[] tax = buffer.getColumnBufferAsFloat(5);
            long[] shipdate = buffer.getColumnBufferAsLong(6);
            for (int i = 0; i < read; ++i) {
                ++allCount;
                if (shipdate[i] > thresholdShipdate) {
                    ++droppedCount;
                    continue;
                }
                result.add(returnflag[i], linestatus[i], quantity[i], price[i], discount[i], tax[i]);
            }
        }
        LOG.info("processed partition. allCount=" + allCount + ", droppedCount=" + droppedCount);
    }
    
    protected final void prepareInputs () throws Exception {
        this.table = context.metaRepo.getTable(parameters.getTableId());
        assert (table != null);
        
        this.columns = new LVColumn[columnNames.length];
        ColumnType[] types = new ColumnType[columnNames.length];
        for (int i = 0; i < columns.length; ++i) {
            columns[i] = context.metaRepo.getColumnByName(table.getTableId(), columnNames[i]);
            if (columns[i] == null) {
                throw new IOException ("column not found:" + columnNames[i]);
            }
            types[i] = columns[i].getType();
        }

        this.partitions = new LVReplicaPartition[parameters.getPartitionIds().length];
        for (int i = 0; i < partitions.length; ++i) {
            partitions[i] = context.metaRepo.getReplicaPartition(parameters.getPartitionIds()[i]);
        }
        
        this.buffer = new TupleBuffer(types, 1 << 18); // quite big buffer
        this.thresholdShipdate = new GregorianCalendar(1998, 12 - 1, 1).getTimeInMillis() - (long) parameters.getDeltaDays() * 1000 * 60 * 24;
    }
}
