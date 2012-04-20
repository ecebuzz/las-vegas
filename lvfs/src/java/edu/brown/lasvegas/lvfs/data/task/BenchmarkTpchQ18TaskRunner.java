package edu.brown.lasvegas.lvfs.data.task;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.ColumnFileReaderBundle;
import edu.brown.lasvegas.lvfs.data.DataTaskRunner;

/**
 * Base class for the two implementations (fast query plan and slower query plan)
 * of TPC-H Q18 Task.
 */
public abstract class BenchmarkTpchQ18TaskRunner extends DataTaskRunner<BenchmarkTpchQ18TaskParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ18TaskRunner.class);
    protected LVTable lineitem, orders;
    protected LVColumn l_orderkey, l_quantity, o_orderkey, o_orderdate, o_totalprice;
    /** num of partitions to be processed in this node. */
    protected int ordersPartitionCount;
    protected LVReplicaPartition ordersPartitions[];
    
    @Override
    protected final String[] runDataTask() throws Exception {
        prepareInputs ();
        double totalSum = 0;
        for (int i = 0; i < ordersPartitionCount; ++i) {
            LOG.info("processing.. " + i + "/" + ordersPartitionCount);
            totalSum += processPartition (i);
        }
        // a hack to make it easy. this return value should be a file path, but let's just return the query result as the string.
        return new String[]{String.valueOf (totalSum)};
    }
    
    protected abstract double processPartition (int partPartition) throws IOException;
    
    // TODO this function should be somewhere in shared place
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
        this.orders = context.metaRepo.getTable(parameters.getOrdersTableId());
        assert (orders != null);
        
        this.l_orderkey = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_orderkey");
        assert (l_orderkey != null);
        this.l_quantity = context.metaRepo.getColumnByName(lineitem.getTableId(), "l_quantity");
        assert (l_quantity != null);

        this.o_orderkey = context.metaRepo.getColumnByName(orders.getTableId(), "o_orderkey");
        assert (o_orderkey != null);
        this.o_orderdate = context.metaRepo.getColumnByName(orders.getTableId(), "o_orderdate");
        assert (o_orderdate != null);
        this.o_totalprice = context.metaRepo.getColumnByName(orders.getTableId(), "o_totalprice");
        assert (o_totalprice != null);

        this.ordersPartitionCount = parameters.getOrdersPartitionIds().length;
        assert (ordersPartitionCount > 0);
        this.ordersPartitions = new LVReplicaPartition[ordersPartitionCount];
        for (int i = 0; i < ordersPartitionCount; ++i) {
        	ordersPartitions[i] = context.metaRepo.getReplicaPartition(parameters.getOrdersPartitionIds()[i]);
        }
        prepareInputsQ18();
    }
    protected abstract void prepareInputsQ18 () throws Exception;
}
