package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Base class for TPC-H Q18 implementation.
 * Plan A (fast query plan utilizing co-partitioning)
 * and Plan B (slow query plan using non-copartitioned files)
 * derive from this.
 * <pre>
SELECT TOP 100 C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,SUM(L_QUANTITY)
FROM LINEITEM
INNER JOIN ORDERS ON (O_ORDERKEY=L_ORDERKEY)
INNER JOIN CUSTOMER ON (C_CUSTKEY=O_CUSTKEY)
WHERE O_ORDERKEY IN (
  SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY)>[QUANTITY]
)
GROUP BY C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE
ORDER BY O_TOTALPRICE DESC, O_ORDERDATE ASC
</pre>
 */
public abstract class BenchmarkTpchQ18JobController extends AbstractJobController<BenchmarkTpchQ18JobParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ18JobController.class);

    public BenchmarkTpchQ18JobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ18JobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    protected LVTable lineitemTable, ordersTable, customerTable;
    protected LVReplicaScheme lineitemScheme, ordersScheme, customerScheme;
    protected LVFracture lineitemFracture, ordersFracture, customerFracture;
    protected LVReplica lineitemReplica, ordersReplica, customerReplica;
    protected LVReplicaPartition lineitemPartitions[], ordersPartitions[], customerPartition;
    @Override
    protected final void initDerived() throws IOException {
        this.lineitemTable = metaRepo.getTable(param.getLineitemTableId());
        assert (lineitemTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getLineitemTableId()).length != 16) {
            throw new IOException ("is this table really lineitem table? :" + lineitemTable);
        }

        this.ordersTable = metaRepo.getTable(param.getOrdersTableId());
        assert (ordersTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getOrdersTableId()).length != 9) {
            throw new IOException ("is this table really orders table? :" + ordersTable);
        }

        this.customerTable = metaRepo.getTable(param.getCustomerTableId());
        assert (customerTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getCustomerTableId()).length != 8) {
            throw new IOException ("is this table really customer table? :" + customerTable);
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(lineitemTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of lineitem table was unexpected:" + fractures.length);
            }
            lineitemFracture = fractures[0];
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(ordersTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of orders table was unexpected:" + fractures.length);
            }
            ordersFracture = fractures[0];
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(customerTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of customer table was unexpected:" + fractures.length);
            }
            customerFracture = fractures[0];
        }
        
        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(lineitemTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            lineitemScheme = schemes[0];
        }

        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(ordersTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            ordersScheme = schemes[0];
        }

        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(customerTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            customerScheme = schemes[0];
        }
        
        lineitemReplica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), lineitemFracture.getFractureId());
        assert (lineitemReplica != null);
        ordersReplica = metaRepo.getReplicaFromSchemeAndFracture(ordersScheme.getSchemeId(), ordersFracture.getFractureId());
        assert (ordersReplica != null);
        customerReplica = metaRepo.getReplicaFromSchemeAndFracture(customerScheme.getSchemeId(), customerFracture.getFractureId());
        assert (customerReplica != null);
        
        lineitemPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(lineitemReplica.getReplicaId());
        ordersPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(ordersReplica.getReplicaId());
        LVReplicaPartition[] customerPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(customerReplica.getReplicaId());
        assert (customerPartitions.length == 1);
        customerPartition = customerPartitions[0];
        
        initDerivedTpchQ18 ();
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q18", JobType.BENCHMARK_TPCH_Q18, null);
    }
    public static class Q18Result {
    	public String C_NAME;
    	public int C_CUSTKEY;
    	public long O_ORDERKEY;
    	public long O_ORDERDATE;
    	public double O_TOTALPRICE;
    	public double SUM_L_QUANTITY;
    }
    protected ArrayList<Q18Result> queryResult;
    public final ArrayList<Q18Result> getQueryResult () {
        return queryResult;
    }
    
    protected abstract void initDerivedTpchQ18() throws IOException;

    // TODO this function should be somewhere in shared place
    protected static int[] asIntArray (List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = list.get(i);
        }
        return array;
    }
}
