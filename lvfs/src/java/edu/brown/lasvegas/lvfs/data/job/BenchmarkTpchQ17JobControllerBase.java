package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;
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
 * Base class for the two implementations (fast query plan and slower query plan)
 * of TPC-H Q17.
 */
public abstract class BenchmarkTpchQ17JobControllerBase extends AbstractJobController<BenchmarkTpchQ17JobParameters> {
    protected static Logger LOG = Logger.getLogger(BenchmarkTpchQ17JobControllerBase.class);
    
    public BenchmarkTpchQ17JobControllerBase (LVMetadataProtocol metaRepo) throws IOException {
        super (metaRepo);
    }
    public BenchmarkTpchQ17JobControllerBase (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }

    protected LVTable lineitemTable, partTable;
    protected LVReplicaScheme lineitemScheme, partScheme;
    protected LVFracture lineitemFracture, partFracture;
    protected LVReplica lineitemReplica, partReplica;
    protected LVReplicaPartition lineitemPartitions[], partPartitions[];

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
            throw new IOException ("is this table really lineitem table? :" + partTable);
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(lineitemTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of lineitem table was unexpected:" + fractures.length);
            }
            lineitemFracture = fractures[0];
        }

        {
            LVFracture[] fractures = metaRepo.getAllFractures(partTable.getTableId());
            if (fractures.length != 1) {
                throw new IOException ("the number of fractures of part table was unexpected:" + fractures.length);
            }
            partFracture = fractures[0];
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
            LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(partTable.getTableId());
            assert (groups.length == 1);
            LVReplicaGroup group = groups[0];
            LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(group.getGroupId());
            assert (schemes.length == 1);
            partScheme = schemes[0];
        }
        
        lineitemReplica = metaRepo.getReplicaFromSchemeAndFracture(lineitemScheme.getSchemeId(), lineitemFracture.getFractureId());
        assert (lineitemReplica != null);
        partReplica = metaRepo.getReplicaFromSchemeAndFracture(partScheme.getSchemeId(), partFracture.getFractureId());
        assert (partReplica != null);
        
        lineitemPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(lineitemReplica.getReplicaId());
        partPartitions = metaRepo.getAllReplicaPartitionsByReplicaId(partReplica.getReplicaId());
        
        initDerivedPartitioning ();
        /*
        if (lineitemPartitions.length != partPartitions.length) {
            throw new IOException ("partition count doesn't match");
        }
        
        for (int i = 0; i < lineitemPartitions.length; ++i) {
            if (lineitemPartitions[i].getNodeId() == null) {
                throw new IOException ("this lineitem partition doesn't have nodeId:" + lineitemPartitions[i]);
            }
            if (partPartitions[i].getNodeId() == null) {
                throw new IOException ("this part partition doesn't have nodeId:" + partPartitions[i]);
            }
            if (lineitemPartitions[i].getNodeId().intValue() != partPartitions[i].getNodeId().intValue()) {
                throw new IOException ("this lineitem and part partitions are not collocated. lineitem:" + lineitemPartitions[i] + ", part:" + partPartitions[i]);
            }
        }
        */
        this.jobId = metaRepo.createNewJobIdOnlyReturn("Q17", JobType.BENCHMARK_TPCH_Q17, null);
    }
    protected abstract void initDerivedPartitioning() throws IOException;
    
    protected double queryResult = 0;
    public final double getQueryResult () {
        return queryResult;
    }

    protected static int[] asIntArray (List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = list.get(i);
        }
        return array;
    }
}
