package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureForeignJobController;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureFromBuddyJobController;
import edu.brown.lasvegas.lvfs.data.job.RecoverFractureJobParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Base implementation for recovery benchmarks.
 */
public abstract class RecoverFractureBenchmark {
    private static final Logger LOG = Logger.getLogger(RecoverFractureBenchmark.class);

    private final LVMetadataProtocol metaRepo;
    private final boolean foreignRecovery;
    private final int lostPartitionCount;
    private final LVRackNode[] allNodes;
    private final LVDatabase database;
    private final LVTable table;
    private final int[] columnIds;
    private final CompressionType[] columnCompressions;
    private final LVFracture fracture;
    private final int fractureCount;
    private final LVReplicaGroup sourceGroup;
    private final LVReplicaScheme sourceScheme;
    private final LVColumn damagedPartitioningColumn, damagedSortingColumn;
    private final LVReplicaGroup damagedPartitionTemplateGroup;
    
    public RecoverFractureBenchmark(LVMetadataProtocol metaRepo, boolean foreignRecovery, int lostPartitionCount) throws IOException {
        this.metaRepo = metaRepo;
        this.foreignRecovery = foreignRecovery;
        this.lostPartitionCount = lostPartitionCount;
        assert (lostPartitionCount > 0);

        ArrayList<LVRackNode> allNodeList = new ArrayList<LVRackNode>();
        for (LVRack rack : metaRepo.getAllRacks()) {
            for (LVRackNode node : metaRepo.getAllRackNodes(rack.getRackId())) {
                allNodeList.add(node);
            }
        }
        this.allNodes = allNodeList.toArray(new LVRackNode[0]);
        
        this.database = metaRepo.getDatabase(DataImportTpchBenchmark.DB_NAME);
        // either works, but let's use order-partitioned lineitem
    	this.table = metaRepo.getTable(database.getDatabaseId(), "lineitem_o");
    	LVColumn[] columns = metaRepo.getAllColumnsExceptEpochColumn(table.getTableId());
    	this.columnIds = new int[columns.length];
    	for (int i = 0; i < columns.length; ++i) {
    	    columnIds[i] = columns[i].getColumnId();
    	}
    	this.columnCompressions = new MiniTPCHLineitem().getDefaultCompressions();
    	if (columnIds.length != columnCompressions.length) {
    	    throw new IOException ("wtf");
    	}

    	LVFracture[] fractures = metaRepo.getAllFractures(table.getTableId());
    	this.fractureCount = fractures.length;
        LOG.info("recovering one fracture out of " + fractureCount + " fractures");
        this.fracture = fractures[0];
        LVReplicaGroup[] groups = metaRepo.getAllReplicaGroups(table.getTableId());
        this.sourceGroup = groups[0];
        LVReplicaScheme[] schemes = metaRepo.getAllReplicaSchemes(groups[0].getGroupId());
        this.sourceScheme = schemes[0];
        if (foreignRecovery) {
            this.damagedPartitioningColumn = metaRepo.getColumnByName(table.getTableId(), "l_partkey");
            this.damagedSortingColumn = metaRepo.getColumnByName(table.getTableId(), "l_partkey");
            LVTable t = metaRepo.getTable(database.getDatabaseId(), "lineitem_p");
            LVReplicaGroup[] g = metaRepo.getAllReplicaGroups(t.getTableId());
            damagedPartitionTemplateGroup = g[0];
        } else {
            this.damagedPartitioningColumn = metaRepo.getColumnByName(table.getTableId(), "l_orderkey");
            this.damagedSortingColumn = metaRepo.getColumnByName(table.getTableId(), "l_suppkey");
            damagedPartitionTemplateGroup = sourceGroup;
        }
        if (damagedPartitioningColumn == null) {
            throw new IOException ("damagedPartitioningColumn is null");
        }
    }
    
    public abstract void tearDown () throws IOException;

    public void exec () throws Exception {
        LOG.info("started");
        long start = System.currentTimeMillis();

        // create a new (dummy) group and scheme
        // no complete rack assignments etc. just create minimal records for the benchmark
        LVReplicaGroup damagedGroup;
        if (foreignRecovery) {
            damagedGroup = metaRepo.createNewReplicaGroup(table, damagedPartitioningColumn, damagedPartitionTemplateGroup.getRanges());
        } else {
            damagedGroup = sourceGroup;
        }
        LVReplicaScheme damagedScheme = metaRepo.createNewReplicaScheme(damagedGroup, damagedSortingColumn, columnIds, columnCompressions);
        LVReplica damagedReplica = metaRepo.createNewReplica(damagedScheme, fracture);
        metaRepo.updateReplicaStatus(damagedReplica, ReplicaStatus.NOT_READY);

        LVReplicaPartition[] damagedPartitions = new LVReplicaPartition[damagedPartitionTemplateGroup.getRanges().length];
        for (int range = 0; range < damagedPartitionTemplateGroup.getRanges().length; ++range) {
            damagedPartitions[range] = metaRepo.createNewReplicaPartition(damagedReplica, range);
            LVRackNode node = allNodes[(range + 1) % allNodes.length]; // the "+1" is to emulate buddy exclusion.
            metaRepo.updateReplicaPartitionNoReturn(damagedPartitions[range].getPartitionId(),
                range < lostPartitionCount ? ReplicaPartitionStatus.LOST : ReplicaPartitionStatus.OK, // only the first lostPartitionCount partitions are lost
                new IntWritable(node.getNodeId()));
        }
        
        RecoverFractureJobParameters params = new RecoverFractureJobParameters();
        params.setDamagedSchemeId(damagedScheme.getSchemeId());
        params.setFractureId(fracture.getFractureId());
        params.setSourceSchemeId(sourceScheme.getSchemeId());

        LOG.info("started Recovery(foreignRecovery=" + foreignRecovery + ", #fractures=" + fractureCount + ",#lostPartitions=" + lostPartitionCount + ")...");
        LVJob job;
        if (foreignRecovery) {
            RecoverFractureForeignJobController controller = new RecoverFractureForeignJobController(metaRepo, 400L, 400L, 100L);
            job = controller.startSync(params);
        } else {
            RecoverFractureFromBuddyJobController controller = new RecoverFractureFromBuddyJobController(metaRepo, 400L, 400L, 100L);
            job = controller.startSync(params);
        }
        LOG.info("finished Recovery(foreignRecovery=" + foreignRecovery + ", #fractures=" + fractureCount + ",#lostPartitions=" + lostPartitionCount + "):" + job);
        for (LVTask task : metaRepo.getAllTasksByJob(job.getJobId())) {
            LOG.info("Sub-Task finished in " + (task.getFinishedTime().getTime() - task.getStartedTime().getTime()) + "ms:" + task);
        }
        
        // destroy the created records.
        // recovered files are left. this is just a benchmark. the files are anyway deleted when it's done 
        if (job.getStatus() == JobStatus.DONE) {
            if (foreignRecovery) {
                metaRepo.dropReplicaGroup(damagedGroup);
            } else {
                metaRepo.dropReplicaScheme(damagedScheme);
            }
        }

        long end = System.currentTimeMillis();
        LOG.info("ended(foreignRecovery=" + foreignRecovery + ", fractureCount=" + fractureCount + ",#lostPartitions=" + lostPartitionCount + "): elapsed time=" + (end - start) + "ms");
    }
}
