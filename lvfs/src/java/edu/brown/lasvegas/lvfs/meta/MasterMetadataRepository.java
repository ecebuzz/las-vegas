package edu.brown.lasvegas.lvfs.meta;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityIndex;

import edu.brown.lasvegas.ColumnStatus;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVSubPartitionScheme;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.TableStatus;
import edu.brown.lasvegas.util.CompositeIntKey;

/**
 * Implementation of {@link MetadataRepository} in the master namenode.
 * This can directly handle all read and write accesses over the local BDB-JE
 * instance.
 */
public class MasterMetadataRepository implements MetadataRepository {
    private static Logger LOG = Logger.getLogger(MasterMetadataRepository.class);
    
    private final File bdbEnvHome;
    private final EnvironmentConfig bdbEnvConf;    
    private final Environment bdbEnv;
    
    private BdbTableAccessors bdbTableAccessors;
    
    private static final long BDB_CACHE_SIZE = 1L << 26;
    
    /**
     * Constructs a metadata repository.
     */
    public MasterMetadataRepository (boolean format, String bdbEnvHomeFolder) throws IOException {
        this.bdbEnvHome = new File(bdbEnvHomeFolder);
        // nuke or create the BDB folder
        if (bdbEnvHome.exists() && format) {
            formatRepository ();
        }
        if (!bdbEnvHome.exists()) {
            LOG.info(bdbEnvHome.getAbsolutePath() + " does not exist. creating..");
            boolean created = bdbEnvHome.mkdirs();
            if (!created) {
                LOG.error(bdbEnvHome.getAbsolutePath() + " could not be created.");
                throw new IOException ("failed to create BDB home folder:" + bdbEnvHome.getAbsolutePath());
            }
            LOG.info(bdbEnvHome.getAbsolutePath() + " has been created.");
        }

        // open the BDB folder
        bdbEnvConf = new EnvironmentConfig();
        bdbEnvConf.setCacheSize(BDB_CACHE_SIZE);
        bdbEnvConf.setAllowCreate(true);

        // prefers performance over full ACID
        bdbEnvConf.setLocking(false);
        bdbEnvConf.setTransactional(false);
        bdbEnvConf.setDurability(Durability.COMMIT_NO_SYNC);

        bdbEnv = new Environment(bdbEnvHome, bdbEnvConf);

        loadRepository();
    }
    
    /** rename the BDB home to cleanup everything. */
    private void formatRepository () throws IOException {
        // we never delete the old repository. just rename.
        File backup = new File(bdbEnvHome.getParentFile(), bdbEnvHome.getName() + "_backup_"
            + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) // append backup-date
            + "_" + new Random(System.nanoTime()).nextInt()); // to make it unique
        LOG.info("renaming the existing BDB folder to " + backup.getAbsolutePath());
        boolean renamed = bdbEnvHome.renameTo(backup);
        if (!renamed) {
            throw new IOException ("failed to backup the old BDB home folder:" + bdbEnvHome.getAbsolutePath());
        }
        LOG.info("renamed as a backup");
    }
    
    /**
     * loads all tables.
     */
    private void loadRepository () {
        bdbTableAccessors = new BdbTableAccessors(bdbEnv);
    }
    
    @Override
    public int issueNewEpoch() throws IOException {
        return bdbTableAccessors.masterTableAccessor.issueNewId(MasterTable.EPOCH_SEQ);
    }

    @Override
    public int issueNewId(Class<?> clazz) throws IOException {
        return bdbTableAccessors.masterTableAccessor.issueNewId(clazz.getName());
    }
    @Override
    public int issueNewIdBlock(Class<?> clazz, int blockSize) throws IOException {
        if (blockSize <= 0) {
            throw new IOException ("invalid blockSize:" + blockSize);
        }
        return bdbTableAccessors.masterTableAccessor.issueNewIdBlock(clazz.getName(), blockSize);
    }

    @Override
    public void sync() throws IOException {
        LOG.info("calling bdbEnv.sync()..");
        bdbEnv.sync();        
        LOG.info("called bdbEnv.sync().");
    }

    @Override
    public void checkpoint() throws IOException {
        LOG.info("calling bdbEnv.checkpoint()..");
        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);
        ckptConfig.setMinimizeRecoveryTime(true);
        bdbEnv.checkpoint(ckptConfig);        
        LOG.info("called bdbEnv.checkpoint().");
        
    }

    @Override
    public void close () throws IOException {
        LOG.info("closing...");
        sync();
        bdbTableAccessors.closeAll();
        bdbEnv.close();
        LOG.info("closed.");
    }

    @Override
    public LVTable getTable(int tableId) throws IOException {
        return bdbTableAccessors.tableAccessor.PKX.get(tableId);
    }
    @Override
    public LVTable getTable(String name) throws IOException {
        return bdbTableAccessors.tableAccessor.IX_NAME.get(name);
    }

    @Override
    public LVTable createNewTable(String name, LVColumn[] columns) throws IOException {
        LOG.info("creating new table " + name + "...");
        // misc parameter check
        if (name == null || name.length() == 0) {
            throw new IOException ("empty table name");
        }
        if (columns.length == 0) {
            throw new IOException ("table without any columns is not allowed");
        }
        if (bdbTableAccessors.tableAccessor.IX_NAME.contains(name)) {
            throw new IOException ("this table name already exists:" + name);
        }

        boolean userSpecifiedFracturingColumn = false;
        {
            // check column name duplicates
            HashSet<String> columnNames = new HashSet<String>();
            columnNames.add(LVColumn.EPOCH_COLUMN_NAME);
            for (LVColumn column : columns) {
                if (column.getName() == null || column.getName().length() == 0) {
                    throw new IOException ("empty column name");
                }
                if (columnNames.contains(column.getName().toLowerCase())) {
                    throw new IOException ("this column name is used more than once:" + column.getName());
                }
                columnNames.add(column.getName().toLowerCase());
                if (column.isFracturingColumn()) {
                    if (userSpecifiedFracturingColumn) {
                        throw new IOException ("cannot specify more than one fracturing column:" + column.getName());
                    }
                    userSpecifiedFracturingColumn = true;
                }
            }
        }

        // first, create table record
        LVTable table = new LVTable();
        table.setName(name);
        table.setStatus(TableStatus.BEING_CREATED);
        int tableId = bdbTableAccessors.tableAccessor.issueNewId();
        table.setTableId(tableId);
        table.setFracturingColumnId(-1); // we don't know this at this point
        bdbTableAccessors.tableAccessor.PKX.putNoReturn(table);
        
        // then, create columns. first column is the implicit epoch column
        int fracturingColumnId = -1;
        {
            LVColumn column = new LVColumn();
            column.setName(LVColumn.EPOCH_COLUMN_NAME);
            column.setOrder(0);
            column.setStatus(ColumnStatus.OK);
            column.setType(ColumnType.INTEGER);
            column.setTableId(tableId);
            int columnId = bdbTableAccessors.columnAccessor.issueNewId();
            column.setColumnId(columnId);
            column.setFracturingColumn(!userSpecifiedFracturingColumn);
            bdbTableAccessors.columnAccessor.PKX.putNoReturn(column);
            if (column.isFracturingColumn()) {
                fracturingColumnId = columnId;
            }
        }
        for (int i = 0; i < columns.length; ++i) {
            int order = i + 1;
            LVColumn column = new LVColumn();
            column.setName(columns[i].getName());
            column.setOrder(order);
            column.setStatus(ColumnStatus.OK);
            column.setType(columns[i].getType());
            column.setTableId(tableId);
            int columnId = bdbTableAccessors.columnAccessor.issueNewId();
            column.setColumnId(columnId);
            column.setFracturingColumn(columns[i].isFracturingColumn());
            bdbTableAccessors.columnAccessor.PKX.putNoReturn(column);
            if (column.isFracturingColumn()) {
                fracturingColumnId = columnId;
            }
        }
        assert (fracturingColumnId != -1);
        
        // finally,  update the table
        table.setFracturingColumnId(fracturingColumnId);
        table.setStatus(TableStatus.OK);
        bdbTableAccessors.tableAccessor.PKX.putNoReturn(table);
        
        LOG.info("created new table");
        return table;
    }

    @Override
    public void requestDropTable(LVTable table) throws IOException {
        assert (table.getTableId() > 0);
        if (LOG.isInfoEnabled()) {
            LOG.info("drop table requested : " + table);
        }
        if (!bdbTableAccessors.tableAccessor.PKX.contains(table.getTableId())) {
            throw new IOException("this table does not exist. already dropped? : " + table);
        }
        table.setStatus(TableStatus.BEING_DROPPED);
        bdbTableAccessors.tableAccessor.PKX.putNoReturn(table);
    }

    @Override
    public void dropTable(LVTable table) throws IOException {
        LOG.info("Dropping table : " + table);
        assert (table.getTableId() > 0);
        // drop child fractures
        LVFracture[] fractures = getAllFractures(table.getTableId());
        for (LVFracture fracture : fractures) {
            dropFracture(fracture);
        }
        // drop child replica group
        LVReplicaGroup[] groups = getAllReplicaGroups(table.getTableId());
        for (LVReplicaGroup group : groups) {
            dropReplicaGroup(group);
        }
        // drop child columns
        LVColumn[] columns = getAllColumns(table.getTableId());
        for (LVColumn column : columns) {
            dropColumn(column);
        }
        boolean deleted = bdbTableAccessors.tableAccessor.PKX.delete(table.getTableId());
        if (!deleted) {
            LOG.warn("this table has been already deleted?? :" + table);
        }
        LOG.info("Dropped");
    }

    @Override
    public LVColumn[] getAllColumns(int tableId) throws IOException {
        EntityIndex<Integer, LVColumn> entities = bdbTableAccessors.columnAccessor.IX_TABLE_ID.subIndex(tableId);
        SortedMap<Integer, LVColumn> orderedMap = new TreeMap<Integer, LVColumn>(); // sorted by Order
        for (LVColumn column : entities.map().values()) {
            assert (!orderedMap.containsKey(column.getOrder()));
            orderedMap.put(column.getOrder(), column);
        }
        return orderedMap.values().toArray(new LVColumn[orderedMap.size()]);
    }

    @Override
    public LVColumn getColumn(int columnId) throws IOException {
        return bdbTableAccessors.columnAccessor.PKX.get(columnId);
    }

    @Override
    public LVColumn createNewColumn(LVTable table, String name, ColumnType type) throws IOException {
        assert (table.getTableId() > 0);
        if (name == null || name.length() == 0) {
            throw new IOException ("empty column name");
        }
        int maxOrder = -1;
        for (LVColumn column : bdbTableAccessors.columnAccessor.IX_TABLE_ID.subIndex(table.getTableId()).map().values()) {
            if (column.getName().equalsIgnoreCase(name)) {
                throw new IOException ("this column name already exists: " + name);
            }
            maxOrder = Math.max(maxOrder, column.getOrder());
        }
        assert (maxOrder > 0);

        LVColumn column = new LVColumn();
        column.setName(name);
        column.setOrder(maxOrder + 1);
        column.setStatus(ColumnStatus.BEING_CREATED);
        column.setType(type);
        column.setTableId(table.getTableId());
        column.setFracturingColumn(false);
        column.setColumnId(bdbTableAccessors.columnAccessor.issueNewId());
        bdbTableAccessors.columnAccessor.PKX.putNoReturn(column);
        return column;
    }

    @Override
    public void requestDropColumn(LVColumn column) throws IOException {
        assert (column.getColumnId() > 0);
        if (LOG.isInfoEnabled()) {
            LOG.info("drop column requested : " + column);
        }
        if (!bdbTableAccessors.columnAccessor.PKX.contains(column.getColumnId())) {
            throw new IOException("this column does not exist. already dropped? : " + column);
        }
        column.setStatus(ColumnStatus.BEING_DROPPED);
        bdbTableAccessors.columnAccessor.PKX.putNoReturn(column);
    }

    @Override
    public void dropColumn(LVColumn column) throws IOException {
        assert (column.getColumnId() > 0);
        boolean deleted = bdbTableAccessors.columnAccessor.PKX.delete(column.getColumnId());
        if (!deleted) {
            LOG.warn("this column has been already deleted?? :" + column);
        }
    }

    @Override
    public LVFracture getFracture(int fractureId) throws IOException {
        return bdbTableAccessors.fractureAccessor.PKX.get(fractureId);
    }

    @Override
    public LVFracture[] getAllFractures(int tableId) throws IOException {
        // ID order
        Collection<LVFracture> values = bdbTableAccessors.fractureAccessor.IX_TABLE_ID.subIndex(tableId).sortedMap().values();
        return values.toArray(new LVFracture[values.size()]);
    }

    @Override
    public LVFracture createNewFracture(LVTable table) throws IOException {
        assert (table.getTableId() > 0);
        LVFracture fracture = new LVFracture();
        fracture.setTableId(table.getTableId());
        fracture.setFractureId(bdbTableAccessors.fractureAccessor.issueNewId());
        bdbTableAccessors.fractureAccessor.PKX.putNoReturn(fracture);
        return fracture;
    }

    @Override
    public void finalizeFracture(LVFracture fracture) throws IOException {
        assert (fracture.getFractureId() > 0);
        bdbTableAccessors.fractureAccessor.PKX.putNoReturn(fracture);
    }

    @Override
    public void dropFracture(LVFracture fracture) throws IOException {
        LOG.info("Dropping fracture : " + fracture);
        assert (fracture.getFractureId() > 0);
        // drop child sub-partition schemes
        LVSubPartitionScheme[] subPartitionSchemes = getAllSubPartitionSchemesByFractureId(fracture.getFractureId());
        for (LVSubPartitionScheme subPartitionScheme : subPartitionSchemes) {
            dropSubPartitionScheme(subPartitionScheme);
        }
        // drop child replicas
        LVReplica[] replicas = getAllReplicasByFractureId(fracture.getFractureId());
        for (LVReplica replica : replicas) {
            dropReplica(replica);
        }
        boolean deleted = bdbTableAccessors.fractureAccessor.PKX.delete(fracture.getFractureId());
        if (!deleted) {
            LOG.warn("this fracture has been already deleted?? :" + fracture);
        }
        LOG.info("Dropped");
    }

    @Override
    public LVReplicaGroup getReplicaGroup(int groupId) throws IOException {
        return bdbTableAccessors.replicaGroupAccessor.PKX.get(groupId);
    }

    @Override
    public LVReplicaGroup[] getAllReplicaGroups(int tableId) throws IOException {
        // ID order
        Collection<LVReplicaGroup> values = bdbTableAccessors.replicaGroupAccessor.IX_TABLE_ID.subIndex(tableId).sortedMap().values();
        return values.toArray(new LVReplicaGroup[values.size()]);
    }

    @Override
    public LVReplicaGroup createNewReplicaGroup(LVTable table, LVColumn partitioningColumn) throws IOException {
        assert (table.getTableId() > 0);
        assert (partitioningColumn.getColumnId() > 0);
        assert (table.getTableId() == partitioningColumn.getTableId());
        // check other group
        for (LVReplicaGroup existing : bdbTableAccessors.replicaGroupAccessor.IX_TABLE_ID.subIndex(table.getTableId()).map().values()) {
            assert (table.getTableId() == existing.getTableId());
            if (existing.getPartitioningColumnId() == partitioningColumn.getColumnId()) {
                throw new IOException ("another replica group with the same partitioning column already exists : " + existing);
            }
        }
        
        LVReplicaGroup group = new LVReplicaGroup();
        group.setPartitioningColumnId(partitioningColumn.getColumnId());
        group.setTableId(table.getTableId());
        group.setGroupId(bdbTableAccessors.replicaGroupAccessor.issueNewId());
        bdbTableAccessors.replicaGroupAccessor.PKX.putNoReturn(group);
        
        return group;
    }

    @Override
    public void dropReplicaGroup(LVReplicaGroup group) throws IOException {
        LOG.info("Dropping replica group : " + group);
        assert (group.getGroupId() > 0);
        // drop child sub-partition schemes
        LVSubPartitionScheme[] subPartitionSchemes = getAllSubPartitionSchemesByGroupId(group.getGroupId());
        for (LVSubPartitionScheme subPartitionScheme : subPartitionSchemes) {
            dropSubPartitionScheme(subPartitionScheme);
        }
        // drop child replica schemes
        LVReplicaScheme[] schemes = getAllReplicaSchemes(group.getGroupId());
        for (LVReplicaScheme scheme : schemes) {
            dropReplicaScheme(scheme);
        }
        boolean deleted = bdbTableAccessors.replicaGroupAccessor.PKX.delete(group.getGroupId());
        if (!deleted) {
            LOG.warn("this replica group has been already deleted?? :" + group);
        }
        LOG.info("Dropped");
    }

    @Override
    public LVReplicaScheme getReplicaScheme(int schemeId) throws IOException {
        return bdbTableAccessors.replicaSchemeAccessor.PKX.get(schemeId);
    }

    @Override
    public LVReplicaScheme[] getAllReplicaSchemes(int groupId) throws IOException {
        // ID order
        Collection<LVReplicaScheme> values = bdbTableAccessors.replicaSchemeAccessor.IX_GROUP_ID.subIndex(groupId).sortedMap().values();
        return values.toArray(new LVReplicaScheme[values.size()]);
    }

    @Override
    public LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVColumn sortingColumn, Map<Integer, CompressionType> columnCompressionSchemes)
                    throws IOException {
        assert (group.getGroupId() > 0);
        assert (sortingColumn.getColumnId() > 0);
        assert (group.getTableId() == sortingColumn.getTableId());
        
        HashMap<Integer, CompressionType> clonedCompressionSchemes = new HashMap<Integer, CompressionType> (columnCompressionSchemes);
        boolean foundSortingColumn = false;
        // complement compression type
        for (LVColumn column : getAllColumns(group.getTableId())) {
            if (column.getColumnId() == sortingColumn.getColumnId()) {
                foundSortingColumn = true;
            }
            if (!clonedCompressionSchemes.containsKey(column.getColumnId())) {
                if (column.getName().equals(LVColumn.EPOCH_COLUMN_NAME)) {
                    // epoch is always RLE because it's a few-valued column
                    clonedCompressionSchemes.put(column.getColumnId(), CompressionType.RLE);
                } else {
                    clonedCompressionSchemes.put(column.getColumnId(), CompressionType.NONE);
                }
            }
        }
        assert (foundSortingColumn);

        LVReplicaScheme scheme = new LVReplicaScheme();
        scheme.setColumnCompressionSchemes(clonedCompressionSchemes);
        scheme.setGroupId(group.getGroupId());
        scheme.setSchemeId(bdbTableAccessors.replicaSchemeAccessor.issueNewId());
        scheme.setSortColumnId(sortingColumn.getColumnId());
        bdbTableAccessors.replicaSchemeAccessor.PKX.putNoReturn(scheme);
        return scheme;
    }

    @Override
    public LVReplicaScheme changeColumnCompressionScheme(LVReplicaScheme scheme, LVColumn column, CompressionType compressionType) throws IOException {
        assert (scheme.getSchemeId() > 0);
        scheme.getColumnCompressionSchemes().put(column.getColumnId(), compressionType);
        bdbTableAccessors.replicaSchemeAccessor.PKX.putNoReturn(scheme);
        return scheme;
    }

    @Override
    public void dropReplicaScheme(LVReplicaScheme scheme) throws IOException {
        assert (scheme.getSchemeId() > 0);
        LVReplica[] replicas = getAllReplicasBySchemeId(scheme.getSchemeId());
        for (LVReplica replica : replicas) {
            dropReplica(replica);
        }
        boolean deleted = bdbTableAccessors.replicaSchemeAccessor.PKX.delete(scheme.getSchemeId());
        if (!deleted) {
            LOG.warn("this replica scheme has been already deleted?? :" + scheme);
        }
    }

    @Override
    public LVReplica getReplica(int replicaId) throws IOException {
        return bdbTableAccessors.replicaAccessor.PKX.get(replicaId);
    }

    @Override
    public LVReplica[] getAllReplicasBySchemeId(int schemeId) throws IOException {
        // ID order
        Collection<LVReplica> values = bdbTableAccessors.replicaAccessor.IX_SCHEME_ID.subIndex(schemeId).sortedMap().values();
        return values.toArray(new LVReplica[values.size()]);
    }

    @Override
    public LVReplica[] getAllReplicasByFractureId(int fractureId) throws IOException {
        // ID order
        Collection<LVReplica> values = bdbTableAccessors.replicaAccessor.IX_FRACTURE_ID.subIndex(fractureId).sortedMap().values();
        return values.toArray(new LVReplica[values.size()]);
    }

    @Override
    public LVReplica getReplicaFromSchemeAndFracture(int schemeId, int fractureId) throws IOException {
        return bdbTableAccessors.replicaAccessor.IX_SCHEME_FRACTURE_ID.get(new CompositeIntKey(schemeId, fractureId));
    }

    @Override
    public LVReplica createNewReplica(LVReplicaScheme scheme, LVFracture fracture) throws IOException {
        assert (scheme.getSchemeId() > 0);
        assert (fracture.getFractureId() > 0);
        LVReplica replica = new LVReplica();
        replica.setFractureId(fracture.getFractureId());
        replica.setSchemeId(scheme.getSchemeId());
        replica.setStatus(ReplicaStatus.NOT_READY);
        replica.setReplicaId(bdbTableAccessors.replicaAccessor.issueNewId());

        // also sets ReplicaPartitionSchemeId. this is a de-normalization
        LVSubPartitionScheme subPartitionScheme = getSubPartitionSchemeByFractureAndGroup(fracture.getFractureId(), scheme.getGroupId());
        replica.setSubPartitionSchemeId(subPartitionScheme.getSubPartitionSchemeId());

        bdbTableAccessors.replicaAccessor.PKX.putNoReturn(replica);
        return replica;
    }

    @Override
    public LVReplica updateReplicaStatus(LVReplica replica, ReplicaStatus status) throws IOException {
        replica.setStatus(status);
        bdbTableAccessors.replicaAccessor.PKX.putNoReturn(replica);
        return replica;
    }

    @Override
    public void dropReplica(LVReplica replica) throws IOException {
        assert (replica.getReplicaId() > 0);
        LVReplicaPartition[] subPartitions = getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
        for (LVReplicaPartition subPartition : subPartitions) {
            dropReplicaPartition(subPartition);
        }
        boolean deleted = bdbTableAccessors.replicaAccessor.PKX.delete(replica.getReplicaId());
        if (!deleted) {
            LOG.warn("this replica has been already deleted?? :" + replica);
        }
    }

    @Override
    public LVSubPartitionScheme getSubPartitionScheme(int subPartitionSchemeId) throws IOException {
        return bdbTableAccessors.subPartitionSchemeAccessor.PKX.get(subPartitionSchemeId);
    }

    @Override
    public LVSubPartitionScheme[] getAllSubPartitionSchemesByFractureId(int fractureId) throws IOException {
        // ID order
        Collection<LVSubPartitionScheme> values = bdbTableAccessors.subPartitionSchemeAccessor.IX_FRACTURE_ID.subIndex(fractureId).sortedMap().values();
        return values.toArray(new LVSubPartitionScheme[values.size()]);
    }

    @Override
    public LVSubPartitionScheme[] getAllSubPartitionSchemesByGroupId(int groupId) throws IOException {
        // ID order
        Collection<LVSubPartitionScheme> values = bdbTableAccessors.subPartitionSchemeAccessor.IX_GROUP_ID.subIndex(groupId).sortedMap().values();
        return values.toArray(new LVSubPartitionScheme[values.size()]);
    }

    @Override
    public LVSubPartitionScheme getSubPartitionSchemeByFractureAndGroup(int fractureId, int groupId) throws IOException {
        // #fractures * #groups should be large.. so, joining two indexes is not so slower than composite index
        Map<Integer, LVSubPartitionScheme> map1 = bdbTableAccessors.subPartitionSchemeAccessor.IX_FRACTURE_ID.subIndex(fractureId).map();
        Map<Integer, LVSubPartitionScheme> map2 = bdbTableAccessors.subPartitionSchemeAccessor.IX_GROUP_ID.subIndex(groupId).map();
        for (LVSubPartitionScheme value : map1.values()) {
            if (map2.containsKey(value.getPrimaryKey())) {
                return value;
            }
        }
        return null;
    }

    @Override
    public LVSubPartitionScheme createNewSubPartitionScheme(LVFracture fracture, LVReplicaGroup group) throws IOException {
        assert (fracture.getFractureId() > 0);
        assert (group.getGroupId() > 0);
        assert (fracture.getTableId() == group.getTableId());
        LVSubPartitionScheme partitionScheme = new LVSubPartitionScheme();
        partitionScheme.setFractureId(fracture.getFractureId());
        partitionScheme.setGroupId(group.getGroupId());
        partitionScheme.setSubPartitionSchemeId(bdbTableAccessors.subPartitionSchemeAccessor.issueNewId());
        bdbTableAccessors.subPartitionSchemeAccessor.PKX.putNoReturn(partitionScheme);
        return partitionScheme;
    }

    @Override
    public void finalizeSubPartitionScheme(LVSubPartitionScheme subPartitionScheme) throws IOException {
        assert (subPartitionScheme.getSubPartitionSchemeId() > 0);
        bdbTableAccessors.subPartitionSchemeAccessor.PKX.putNoReturn(subPartitionScheme);
    }

    @Override
    public void dropSubPartitionScheme(LVSubPartitionScheme subPartitionScheme) throws IOException {
        assert (subPartitionScheme.getSubPartitionSchemeId() > 0);
        boolean deleted = bdbTableAccessors.subPartitionSchemeAccessor.PKX.delete(subPartitionScheme.getSubPartitionSchemeId());
        if (!deleted) {
            LOG.warn("this sub-partition scheme has been already deleted?? :" + subPartitionScheme);
        }
    }

    @Override
    public LVReplicaPartition getReplicaPartition(int subPartitionId) throws IOException {
        return bdbTableAccessors.replicaPartitionAccessor.PKX.get(subPartitionId);
    }

    @Override
    public LVReplicaPartition[] getAllReplicaPartitionsByReplicaId(int replicaId) throws IOException {
        Collection<LVReplicaPartition> values = bdbTableAccessors.replicaPartitionAccessor.IX_REPLICA_ID.subIndex(replicaId).map().values();
        // range order
        SortedMap<Integer, LVReplicaPartition> map = new TreeMap<Integer, LVReplicaPartition>();
        for (LVReplicaPartition value : values) {
            assert (value.getRange() >= 0);
            assert (!map.containsKey(value.getRange()));
            map.put(value.getRange(), value);
        }
        return map.values().toArray(new LVReplicaPartition[values.size()]);
    }

    @Override
    public LVReplicaPartition getReplicaPartitionByReplicaAndRange(int replicaId, int range) throws IOException {
        return bdbTableAccessors.replicaPartitionAccessor.IX_REPLICA_RANGE.get(new CompositeIntKey(replicaId, range));
    }

    @Override
    public LVReplicaPartition createNewReplicaPartition(LVReplica replica, int range) throws IOException {
        assert (replica.getReplicaId() > 0);
        assert (range >= 0);
        LVReplicaPartition subPartition = new LVReplicaPartition();
        subPartition.setRange(range);
        subPartition.setReplicaId(replica.getReplicaId());
        subPartition.setStatus(ReplicaPartitionStatus.BEING_RECOVERED);
        subPartition.setPartitionId(bdbTableAccessors.replicaPartitionAccessor.issueNewId());
        // also sets ReplicaPartitionSchemeId. this is a de-normalization
        subPartition.setSubPartitionSchemeId(replica.getSubPartitionSchemeId());

        bdbTableAccessors.replicaPartitionAccessor.PKX.putNoReturn(subPartition);
        return subPartition;
    }

    @Override
    public LVReplicaPartition updateReplicaPartition(LVReplicaPartition subPartition, ReplicaPartitionStatus status,
                    String currentHdfsNodeUri, String recoveryHdfsNodeUri)
                    throws IOException {
        assert (subPartition.getPartitionId() > 0);
        subPartition.setStatus(status);
        subPartition.setCurrentHdfsNodeUri(currentHdfsNodeUri);
        subPartition.setRecoveryHdfsNodeUri(recoveryHdfsNodeUri);
        bdbTableAccessors.replicaPartitionAccessor.PKX.putNoReturn(subPartition);
        return subPartition;
    }

    @Override
    public void dropReplicaPartition(LVReplicaPartition subPartition) throws IOException {
        assert (subPartition.getPartitionId() > 0);
        LVColumnFile[] columnFiles = getAllColumnFilesByReplicaPartitionId (subPartition.getPartitionId());
        for (LVColumnFile columnFile : columnFiles) {
            dropColumnFile(columnFile);
        }
        boolean deleted = bdbTableAccessors.replicaPartitionAccessor.PKX.delete(subPartition.getPartitionId());
        if (!deleted) {
            LOG.warn("this sub-partition has been already deleted?? :" + subPartition);
        }
    }

    @Override
    public LVColumnFile getColumnFile(int columnFileId) throws IOException {
        return bdbTableAccessors.columnFileAccessor.PKX.get(columnFileId);
    }

    @Override
    public LVColumnFile[] getAllColumnFilesByReplicaPartitionId(int subPartitionId) throws IOException {
        Collection<LVColumnFile> values = bdbTableAccessors.columnFileAccessor.IX_PARTITION_ID.subIndex(subPartitionId).sortedMap().values();
        // TODO sort by column's order... although this is not quite an important contract
        return values.toArray(new LVColumnFile[values.size()]);
    }

    @Override
    public LVColumnFile getColumnFileByReplicaPartitionAndColumn(int subPartitionId, int columnId) throws IOException {
        return bdbTableAccessors.columnFileAccessor.IX_PARTITION_COLUMN_ID.get(new CompositeIntKey(subPartitionId, columnId));
    }

    @Override
    public LVColumnFile createNewColumnFile(LVReplicaPartition subPartition, LVColumn column, String hdfsFilePath, long fileSize, int checksum) throws IOException {
        assert (column.getColumnId() > 0);
        assert (subPartition.getPartitionId() > 0);
        LVColumnFile file = new LVColumnFile();
        file.setColumnFileId(bdbTableAccessors.columnFileAccessor.issueNewId());
        file.setColumnId(column.getColumnId());
        file.setFileSize(fileSize);
        file.setHdfsFilePath(hdfsFilePath);
        file.setPartitionId(subPartition.getPartitionId());
        file.setChecksum(checksum);
        bdbTableAccessors.columnFileAccessor.PKX.putNoReturn(file);
        return file;
    }

    @Override
    public void dropColumnFile(LVColumnFile columnFile) throws IOException {
        assert (columnFile.getColumnFileId() > 0);
        boolean deleted = bdbTableAccessors.columnFileAccessor.PKX.delete(columnFile.getColumnFileId());
        if (!deleted) {
            LOG.warn("this column file has been already deleted?? :" + columnFile);
        }
    }
}