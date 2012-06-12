package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;

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
 * Base class for the two implementations (fast query plan and slower query plan)
 * of TPC-H Q17.
 * Part table must have only one fracture while lineitem table can have
 * an arbitrary number of fractures.
 * <pre>
 SELECT SUM(L_EXTENDEDPRICE) / 7 FROM LINEITEM JOIN PART ON (P_PARTKEY=L_PARTKEY)
 WHERE P_BRAND=[BRAND] AND P_CONTAINER=[CONTAINER] AND L_QUANTITY<
 (
   SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY=P_PARTKEY
 )
 </pre>
 */
public abstract class BenchmarkTpchQ17JobController extends AbstractJobController<BenchmarkTpchQ17JobParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ17JobController.class);
    
    public BenchmarkTpchQ17JobController (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ17JobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    protected LVTable lineitemTable, partTable;
    protected LVReplicaScheme lineitemScheme, partScheme;
    protected LVFracture partFracture;
    protected LVReplicaPartition[] partPartitions;
    protected LVFracture[] lineitemFractures;
    protected LVReplicaPartition[][] lineitemPartitionLists; // first array index shared with lineitemFractures

    @Override
    protected final void initDerived() throws IOException {
        this.lineitemTable = metaRepo.getTable(param.getLineitemTableId());
        assert (lineitemTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getLineitemTableId()).length != 16) {
            throw new IOException ("is this table really lineitem table? :" + lineitemTable);
        }

        this.partTable = metaRepo.getTable(param.getPartTableId());
        assert (partTable != null);
        if (metaRepo.getAllColumnsExceptEpochColumn(param.getPartTableId()).length != 9) {
            throw new IOException ("is this table really part table? :" + partTable);
        }

        lineitemFractures = metaRepo.getAllFractures(lineitemTable.getTableId());
        {
            LVFracture[] fractures = metaRepo.getAllFractures(partTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of part table was unexpected:" + fractures.length);
            }
            partFracture = fractures[0];
        }
        
        // TODO: so far this job assumes only one replica group and scheme in the table.
        // this should be a parameter of the job so that the caller can specify which replica scheme to use.
        // trivial to implement and so far not needed, so just a todo...
        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(lineitemTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            lineitemScheme = schemes[0];
        }

        {
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(partTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            partScheme = schemes[0];
        }
        
        LVReplica partReplica = metaRepo.getReplicaFromSchemeAndFracture(partScheme.getSchemeId(), partFracture.getFractureId());
        assert (partReplica != null);
        partPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(partReplica.getReplicaId());

        lineitemPartitionLists = new LVReplicaPartition[lineitemFractures.length][];
        for (int i = 0; i < lineitemFractures.length; ++i) {
            LVFracture fracture = lineitemFractures[i];
            LVReplica replica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), fracture.getFractureId());
            assert (replica != null);
            lineitemPartitionLists[i] = metaRepo.getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
        }
        
        initDerivedTpchQ17 ();
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q17", JobType.BENCHMARK_TPCH_Q17, null);
    }
    protected abstract void initDerivedTpchQ17() throws IOException;
    
    protected double queryResult = 0;
    public final double getQueryResult () {
        return queryResult;
    }
}
