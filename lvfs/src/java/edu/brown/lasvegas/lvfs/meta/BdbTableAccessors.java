package edu.brown.lasvegas.lvfs.meta;

import org.apache.log4j.Logger;

import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVObjectType;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackAssignment;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVSubPartitionScheme;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.util.CompositeIntKey;
import edu.brown.lasvegas.util.MemoryUtil;

/**
 * Collection of accessors for BDB tables.
 */
class BdbTableAccessors {
    private static Logger LOG = Logger.getLogger(BdbTableAccessors.class);
    final Environment bdbEnv;
    final StoreConfig storeConfig;
    final EntityStore store;
    final MasterTableAccessor masterTableAccessor;

    final TableAccessor tableAccessor;
    final ColumnAccessor columnAccessor;
    final FractureAccessor fractureAccessor;
    final RackAccessor rackAccessor;
    final RackNodeAccessor rackNodeAccessor;
    final RackAssignmentAccessor rackAssignmentAccessor;
    final ReplicaGroupAccessor replicaGroupAccessor;
    final ReplicaSchemeAccessor replicaSchemeAccessor;
    final SubPartitionSchemeAccessor subPartitionSchemeAccessor;
    final ReplicaPartitionAccessor replicaPartitionAccessor;
    final ReplicaAccessor replicaAccessor;
    final ColumnFileAccessor columnFileAccessor;
    BdbTableAccessors (Environment bdbEnv) {
        this.bdbEnv = bdbEnv;
        if (LOG.isInfoEnabled()) {
            LOG.info("loading BDB. stat=" + bdbEnv.getStats(null));
            MemoryUtil.outputMemory();
        }
        storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(false);
        store = new EntityStore(bdbEnv, MasterTable.DBNAME, storeConfig);
        masterTableAccessor = new MasterTableAccessor(store);
        tableAccessor = new TableAccessor();
        columnAccessor = new ColumnAccessor();
        fractureAccessor = new FractureAccessor();
        rackAccessor = new RackAccessor();
        rackNodeAccessor = new RackNodeAccessor();
        rackAssignmentAccessor = new RackAssignmentAccessor();
        replicaGroupAccessor = new ReplicaGroupAccessor();
        replicaSchemeAccessor = new ReplicaSchemeAccessor();
        subPartitionSchemeAccessor = new SubPartitionSchemeAccessor();
        replicaPartitionAccessor = new ReplicaPartitionAccessor();
        replicaAccessor = new ReplicaAccessor();
        columnFileAccessor = new ColumnFileAccessor();
    }
    
    void closeAll () {
        store.close();
    }

    /**
     * Base class of table accessors for BDB.
     * @param <Ent> entity class (e.g., LVTable).
     */
    abstract class MetaTableAccessor<Ent> {
        MetaTableAccessor (Class<Ent> entClass) {
            PKX = store.getPrimaryIndex(Integer.class, entClass);
            // load everything in main memory. metadata should be enough compact.
            // note: database.preload() is not enough as it only preloads index node.
            if (LOG.isInfoEnabled()) {
                LOG.info("preloading " + store.getStoreName());
            }
            EntityCursor<Ent> cursor = PKX.entities();
            int count = 0;
            while (cursor.next() != null) {
                ++count;
            }
            cursor.close();
            if (LOG.isInfoEnabled()) {
                LOG.info("preloaded " + count + " records");
                MemoryUtil.outputMemory();
            }
        }
        /**
         * primary index on the table.
         */
        final PrimaryIndex<Integer, Ent> PKX;
        abstract LVObjectType getType();
        final int issueNewId () {
            int newId = masterTableAccessor.issueNewId(getType().ordinal());
            if (LOG.isDebugEnabled()) {
                LOG.debug("new id=" + newId);
                Ent existing = PKX.get(newId);
                if (existing != null) {
                    LOG.error("the newly issued id already exists??? : " + existing);
                }
            }
            return newId;
        }
    }

    class TableAccessor extends MetaTableAccessor<LVTable> {
        TableAccessor () {
            super(LVTable.class);
            IX_NAME = store.getSecondaryIndex(PKX, String.class, LVTable.IX_NAME);
        }
        LVObjectType getType() { return LVObjectType.TABLE;}
        final SecondaryIndex<String, Integer, LVTable> IX_NAME;
    }

    class ColumnAccessor extends MetaTableAccessor<LVColumn> {
        ColumnAccessor () {
            super(LVColumn.class);
            IX_TABLE_ID = store.getSecondaryIndex(PKX, Integer.class, LVColumn.IX_TABLE_ID);
        }
        LVObjectType getType() { return LVObjectType.COLUMN;}
        final SecondaryIndex<Integer, Integer, LVColumn> IX_TABLE_ID;
    }

    class FractureAccessor extends MetaTableAccessor<LVFracture> {
        FractureAccessor () {
            super(LVFracture.class);
            IX_TABLE_ID = store.getSecondaryIndex(PKX, Integer.class, LVFracture.IX_TABLE_ID);
        }
        LVObjectType getType() { return LVObjectType.FRACTURE;}
        final SecondaryIndex<Integer, Integer, LVFracture> IX_TABLE_ID;
    }

    class ReplicaGroupAccessor extends MetaTableAccessor<LVReplicaGroup> {
        ReplicaGroupAccessor () {
            super(LVReplicaGroup.class);
            IX_TABLE_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaGroup.IX_TABLE_ID);
        }
        LVObjectType getType() { return LVObjectType.REPLICA;}
        final SecondaryIndex<Integer, Integer, LVReplicaGroup> IX_TABLE_ID;
    }

    class RackAccessor extends MetaTableAccessor<LVRack> {
        RackAccessor () {
            super(LVRack.class);
            IX_NAME = store.getSecondaryIndex(PKX, String.class, LVRack.IX_NAME);
        }
        LVObjectType getType() { return LVObjectType.RACK;}
       final SecondaryIndex<String, Integer, LVRack> IX_NAME;
    }

    class RackNodeAccessor extends MetaTableAccessor<LVRackNode> {
        RackNodeAccessor () {
            super(LVRackNode.class);
            IX_NAME = store.getSecondaryIndex(PKX, String.class, LVRackNode.IX_NAME);
            IX_RACK_ID = store.getSecondaryIndex(PKX, Integer.class, LVRackNode.IX_RACK_ID);
        }
        LVObjectType getType() { return LVObjectType.RACK_NODE;}
        final SecondaryIndex<String, Integer, LVRackNode> IX_NAME;
        final SecondaryIndex<Integer, Integer, LVRackNode> IX_RACK_ID;
    }

    class RackAssignmentAccessor extends MetaTableAccessor<LVRackAssignment> {
        RackAssignmentAccessor () {
            super(LVRackAssignment.class);
            IX_FRACTURE_ID = store.getSecondaryIndex(PKX, Integer.class, LVRackAssignment.IX_FRACTURE_ID);
            IX_RACK_ID = store.getSecondaryIndex(PKX, Integer.class, LVRackAssignment.IX_RACK_ID);
            IX_OWNER = store.getSecondaryIndex(PKX, Integer.class, LVRackAssignment.IX_OWNER);
        }
        LVObjectType getType() { return LVObjectType.RACK_ASSIGNMENT;}
        final SecondaryIndex<Integer, Integer, LVRackAssignment> IX_FRACTURE_ID;
        final SecondaryIndex<Integer, Integer, LVRackAssignment> IX_RACK_ID;
        final SecondaryIndex<Integer, Integer, LVRackAssignment> IX_OWNER;
    }

    class ReplicaSchemeAccessor extends MetaTableAccessor<LVReplicaScheme> {
        ReplicaSchemeAccessor () {
            super(LVReplicaScheme.class);
            IX_GROUP_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaScheme.IX_GROUP_ID);
        }
        LVObjectType getType() { return LVObjectType.REPLICA_SCHEME;}
        final SecondaryIndex<Integer, Integer, LVReplicaScheme> IX_GROUP_ID;
    }

    class SubPartitionSchemeAccessor extends MetaTableAccessor<LVSubPartitionScheme> {
        SubPartitionSchemeAccessor () {
            super(LVSubPartitionScheme.class);
            IX_GROUP_ID = store.getSecondaryIndex(PKX, Integer.class, LVSubPartitionScheme.IX_GROUP_ID);
            IX_FRACTURE_ID = store.getSecondaryIndex(PKX, Integer.class, LVSubPartitionScheme.IX_FRACTURE_ID);
        }
        LVObjectType getType() { return LVObjectType.SUB_PARTITION_SCHEME;}
        final SecondaryIndex<Integer, Integer, LVSubPartitionScheme> IX_GROUP_ID;
        final SecondaryIndex<Integer, Integer, LVSubPartitionScheme> IX_FRACTURE_ID;
    }

    class ReplicaPartitionAccessor extends MetaTableAccessor<LVReplicaPartition> {
        ReplicaPartitionAccessor () {
            super(LVReplicaPartition.class);
            IX_REPLICA_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaPartition.IX_REPLICA_ID);
            IX_REPLICA_RANGE = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVReplicaPartition.IX_REPLICA_RANGE);
            IX_STATUS = store.getSecondaryIndex(PKX, ReplicaPartitionStatus.class, LVReplicaPartition.IX_STATUS);
            IX_NODE_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaPartition.IX_NODE_ID);
        }
        LVObjectType getType() { return LVObjectType.REPLICA_PARTITION;}
        final SecondaryIndex<Integer, Integer, LVReplicaPartition> IX_REPLICA_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVReplicaPartition> IX_REPLICA_RANGE;
        final SecondaryIndex<ReplicaPartitionStatus, Integer, LVReplicaPartition> IX_STATUS;
        final SecondaryIndex<Integer, Integer, LVReplicaPartition> IX_NODE_ID;
    }

    class ReplicaAccessor extends MetaTableAccessor<LVReplica> {
        ReplicaAccessor () {
            super(LVReplica.class);
            IX_FRACTURE_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplica.IX_FRACTURE_ID);
            IX_SCHEME_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplica.IX_SCHEME_ID);
            IX_SCHEME_FRACTURE_ID = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVReplica.IX_SCHEME_FRACTURE_ID);
            IX_STATUS = store.getSecondaryIndex(PKX, ReplicaStatus.class, LVReplica.IX_SCHEME_ID);
        }
        LVObjectType getType() { return LVObjectType.REPLICA;}
        final SecondaryIndex<Integer, Integer, LVReplica> IX_FRACTURE_ID;
        final SecondaryIndex<Integer, Integer, LVReplica> IX_SCHEME_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVReplica> IX_SCHEME_FRACTURE_ID;
        final SecondaryIndex<ReplicaStatus, Integer, LVReplica> IX_STATUS;
    }

    class ColumnFileAccessor extends MetaTableAccessor<LVColumnFile> {
        ColumnFileAccessor () {
            super(LVColumnFile.class);
            IX_PARTITION_ID = store.getSecondaryIndex(PKX, Integer.class, LVColumnFile.IX_PARTITION_ID);
            IX_PARTITION_COLUMN_ID = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVColumnFile.IX_PARTITION_COLUMN_ID);
        }
        LVObjectType getType() { return LVObjectType.COLUMN_FILE;}
        final SecondaryIndex<Integer, Integer, LVColumnFile> IX_PARTITION_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVColumnFile> IX_PARTITION_COLUMN_ID;
    }
}
