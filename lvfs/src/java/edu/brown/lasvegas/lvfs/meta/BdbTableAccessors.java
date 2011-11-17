package edu.brown.lasvegas.lvfs.meta;

import org.apache.log4j.Logger;

import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaPartitionScheme;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTableColumn;
import edu.brown.lasvegas.LVTableFracture;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.util.CompositeIntKey;

/**
 * Collection of accessors for BDB tables.
 */
class BdbTableAccessors {
    private static Logger LOG = Logger.getLogger(BdbTableAccessors.class);
    final Environment bdbEnv;
    final StoreConfig storeConfig;
    final MasterTableAccessor masterTableAccessor;

    BdbTableAccessors (Environment bdbEnv) {
        this.bdbEnv = bdbEnv;
        storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(false);
        EntityStore masterStore = new EntityStore(bdbEnv, MasterTable.DBNAME, storeConfig);
        masterTableAccessor = new MasterTableAccessor(masterStore);
    }

    /**
     * Base class of table accessors for BDB.
     * @param <Ent> entity class (e.g., LVTable).
     */
    class MetaTableAccessor<Ent> {
        MetaTableAccessor (Class<Ent> entClass) {
            idSequence = entClass.getName();
            store = new EntityStore(bdbEnv, "LVFS_" + entClass.getName(), storeConfig);
            PKX = store.getPrimaryIndex(Integer.class, entClass);
        }
        /** name of the ID sequence stored in master table. */
        final String idSequence;
        /** BDB table entity. */
        final EntityStore store;
        /**
         * primary index on the table.
         */
        final PrimaryIndex<Integer, Ent> PKX;
        int issueNewId () {
            int newId = masterTableAccessor.issueNewId(idSequence);
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

    final TableAccessor tableAccessor = new TableAccessor();
    class TableAccessor extends MetaTableAccessor<LVTable> {
        TableAccessor () {
            super(LVTable.class);
            IX_NAME = store.getSecondaryIndex(PKX, String.class, LVTable.IX_NAME);
        }
        final SecondaryIndex<String, Integer, LVTable> IX_NAME;
    }

    final TableColumnAccessor tableColumnAccessor = new TableColumnAccessor();
    class TableColumnAccessor extends MetaTableAccessor<LVTableColumn> {
        TableColumnAccessor () {
            super(LVTableColumn.class);
            IX_TABLE_ID = store.getSecondaryIndex(PKX, Integer.class, LVTableColumn.IX_TABLE_ID);
        }
        final SecondaryIndex<Integer, Integer, LVTableColumn> IX_TABLE_ID;
    }

    final TableFractureAccessor tableFractureAccessor = new TableFractureAccessor();
    class TableFractureAccessor extends MetaTableAccessor<LVTableFracture> {
        TableFractureAccessor () {
            super(LVTableFracture.class);
            IX_TABLE_ID = store.getSecondaryIndex(PKX, Integer.class, LVTableFracture.IX_TABLE_ID);
        }
        final SecondaryIndex<Integer, Integer, LVTableFracture> IX_TABLE_ID;
    }


    final ReplicaGroupAccessor replicaGroupAccessor = new ReplicaGroupAccessor();
    class ReplicaGroupAccessor extends MetaTableAccessor<LVReplicaGroup> {
        ReplicaGroupAccessor () {
            super(LVReplicaGroup.class);
            IX_TABLE_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaGroup.IX_TABLE_ID);
        }
        final SecondaryIndex<Integer, Integer, LVReplicaGroup> IX_TABLE_ID;
    }

    final ReplicaSchemeAccessor replicaSchemeAccessor = new ReplicaSchemeAccessor();
    class ReplicaSchemeAccessor extends MetaTableAccessor<LVReplicaScheme> {
        ReplicaSchemeAccessor () {
            super(LVReplicaScheme.class);
            IX_GROUP_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaScheme.IX_GROUP_ID);
        }
        final SecondaryIndex<Integer, Integer, LVReplicaScheme> IX_GROUP_ID;
    }

    final ReplicaPartitionSchemeAccessor replicaPartitionSchemeAccessor = new ReplicaPartitionSchemeAccessor();
    class ReplicaPartitionSchemeAccessor extends MetaTableAccessor<LVReplicaPartitionScheme> {
        ReplicaPartitionSchemeAccessor () {
            super(LVReplicaPartitionScheme.class);
            IX_GROUP_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaPartitionScheme.IX_GROUP_ID);
            IX_FRACTURE_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaPartitionScheme.IX_FRACTURE_ID);
            IX_FRACTURE_GROUP_ID = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVReplicaPartitionScheme.IX_FRACTURE_GROUP_ID);
        }
        final SecondaryIndex<Integer, Integer, LVReplicaPartitionScheme> IX_GROUP_ID;
        final SecondaryIndex<Integer, Integer, LVReplicaPartitionScheme> IX_FRACTURE_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVReplicaPartitionScheme> IX_FRACTURE_GROUP_ID;
    }

    final ReplicaPartitionAccessor replicaPartitionAccessor = new ReplicaPartitionAccessor();
    class ReplicaPartitionAccessor extends MetaTableAccessor<LVReplicaPartition> {
        ReplicaPartitionAccessor () {
            super(LVReplicaPartition.class);
            IX_REPLICA_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplicaPartition.IX_REPLICA_ID);
            IX_REPLICA_RANGE = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVReplicaPartition.IX_REPLICA_RANGE);
            IX_STATUS = store.getSecondaryIndex(PKX, ReplicaPartitionStatus.class, LVReplicaPartition.IX_STATUS);
        }
        final SecondaryIndex<Integer, Integer, LVReplicaPartition> IX_REPLICA_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVReplicaPartition> IX_REPLICA_RANGE;
        final SecondaryIndex<ReplicaPartitionStatus, Integer, LVReplicaPartition> IX_STATUS;
    }

    final ReplicaAccessor replicaAccessor = new ReplicaAccessor();
    class ReplicaAccessor extends MetaTableAccessor<LVReplica> {
        ReplicaAccessor () {
            super(LVReplica.class);
            IX_FRACTURE_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplica.IX_FRACTURE_ID);
            IX_SCHEME_ID = store.getSecondaryIndex(PKX, Integer.class, LVReplica.IX_SCHEME_ID);
            IX_SCHEME_FRACTURE_ID = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVReplica.IX_SCHEME_FRACTURE_ID);
            IX_STATUS = store.getSecondaryIndex(PKX, ReplicaStatus.class, LVReplica.IX_SCHEME_ID);
        }
        final SecondaryIndex<Integer, Integer, LVReplica> IX_FRACTURE_ID;
        final SecondaryIndex<Integer, Integer, LVReplica> IX_SCHEME_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVReplica> IX_SCHEME_FRACTURE_ID;
        final SecondaryIndex<ReplicaStatus, Integer, LVReplica> IX_STATUS;
    }


    final ColumnFileAccessor columnFileAccessor = new ColumnFileAccessor();
    class ColumnFileAccessor extends MetaTableAccessor<LVColumnFile> {
        ColumnFileAccessor () {
            super(LVColumnFile.class);
            IX_PARTITION_ID = store.getSecondaryIndex(PKX, Integer.class, LVColumnFile.IX_PARTITION_ID);
            IX_PARTITION_COLUMN_ID = store.getSecondaryIndex(PKX, CompositeIntKey.class, LVColumnFile.IX_PARTITION_COLUMN_ID);
        }
        final SecondaryIndex<Integer, Integer, LVColumnFile> IX_PARTITION_ID;
        final SecondaryIndex<CompositeIntKey, Integer, LVColumnFile> IX_PARTITION_COLUMN_ID;
    }
}
