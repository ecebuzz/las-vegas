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
import edu.brown.lasvegas.LVReplicaPartitionScheme;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTableColumn;
import edu.brown.lasvegas.LVTableFracture;
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
            boolean created = bdbEnvHome.mkdir();
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
    
    /** renamed the BDB home to cleanup everything. */
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
     * loads or initializes essential tables.
     */
    private void loadRepository () {
        if (!bdbEnv.getDatabaseNames().contains(MasterTable.DBNAME)) {
            initialiseRepository();
        }
        bdbTableAccessors = new BdbTableAccessors(bdbEnv);
    }
    
    private void initialiseRepository () {
        LOG.info("initializing BDB ...");
        for (String databaseName : bdbEnv.getDatabaseNames()) {
            LOG.info("removing existing DB:" + databaseName);
            bdbEnv.removeDatabase(null, databaseName);
        }
        LOG.info("initialized BDB.");
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
    public LVTable getTable(int tableId) throws IOException {
        return bdbTableAccessors.tableAccessor.PKX.get(tableId);
    }

    @Override
    public LVTable createNewTable(String name, LVTableColumn[] columns, int basePartitioningColumnOrder) throws IOException {
        LOG.info("creating new table " + name + "...");
        // misc parameter check
        if (name == null || name.length() == 0) {
            throw new IOException ("empty table name");
        }
        if (basePartitioningColumnOrder < 0 || basePartitioningColumnOrder > columns.length) {
            throw new IOException ("basePartitioningColumnOrder has an invalid value");
        }
        if (columns.length == 0) {
            throw new IOException ("table without any columns is not allowed");
        }
        if (bdbTableAccessors.tableAccessor.IX_NAME.contains(name)) {
            throw new IOException ("this table name already exists:" + name);
        }
        
        {
            // check column name duplicates
            HashSet<String> columnNames = new HashSet<String>();
            columnNames.add(LVTableColumn.EPOCH_COLUMN_NAME);
            for (LVTableColumn column : columns) {
                if (column.getName() == null || column.getName().length() == 0) {
                    throw new IOException ("empty column name");
                }
                if (columnNames.contains(column.getName().toLowerCase())) {
                    throw new IOException ("this column name is used more than once:" + column.getName());
                }
                columnNames.add(column.getName().toLowerCase());
            }
        }

        // first, create table record
        LVTable table = new LVTable();
        table.setName(name);
        table.setStatus(TableStatus.BEING_CREATED);
        int tableId = bdbTableAccessors.tableAccessor.issueNewId();
        table.setTableId(tableId);
        bdbTableAccessors.tableAccessor.PKX.putNoReturn(table);
        
        // then, create columns. first column is the implicit epoch column
        int basePartitioningColumnId = -1;
        {
            LVTableColumn column = new LVTableColumn();
            column.setName(LVTableColumn.EPOCH_COLUMN_NAME);
            column.setOrder(0);
            column.setStatus(ColumnStatus.OK);
            column.setType(ColumnType.INTEGER);
            column.setTableId(tableId);
            int columnId = bdbTableAccessors.tableColumnAccessor.issueNewId();
            column.setColumnId(columnId);
            bdbTableAccessors.tableColumnAccessor.PKX.putNoReturn(column);
            if (basePartitioningColumnOrder == 0) {
                basePartitioningColumnId = columnId;
            }
        }
        for (int i = 0; i < columns.length; ++i) {
            int order = i + 1;
            LVTableColumn column = new LVTableColumn();
            column.setName(columns[i].getName());
            column.setOrder(order);
            column.setStatus(ColumnStatus.OK);
            column.setType(columns[i].getType());
            column.setTableId(tableId);
            int columnId = bdbTableAccessors.tableColumnAccessor.issueNewId();
            column.setColumnId(columnId);
            bdbTableAccessors.tableColumnAccessor.PKX.putNoReturn(column);
            if (basePartitioningColumnOrder == order) {
                basePartitioningColumnId = columnId;
            }
        }
        assert (basePartitioningColumnId != -1);
        
        // create base replica group
        LVReplicaGroup baseGroup = new LVReplicaGroup();
        baseGroup.setBaseGroup(true);
        baseGroup.setPartitioningColumnId(basePartitioningColumnId);
        baseGroup.setTableId(tableId);
        baseGroup.setGroupId(bdbTableAccessors.replicaGroupAccessor.issueNewId());
        bdbTableAccessors.replicaGroupAccessor.PKX.putNoReturn(baseGroup);
        
        // finally,  update the status of table
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
        // TODO Auto-generated method stub
    }

    @Override
    public LVTableColumn[] getAllColumns(int tableId) throws IOException {
        EntityIndex<Integer, LVTableColumn> entities = bdbTableAccessors.tableColumnAccessor.IX_TABLE_ID.subIndex(tableId);
        SortedMap<Integer, LVTableColumn> orderedMap = new TreeMap<Integer, LVTableColumn>(); // sorted by Order
        for (LVTableColumn column : entities.map().values()) {
            assert (!orderedMap.containsKey(column.getOrder()));
            orderedMap.put(column.getOrder(), column);
        }
        return orderedMap.values().toArray(new LVTableColumn[orderedMap.size()]);
    }

    @Override
    public LVTableColumn getColumn(int columnId) throws IOException {
        return bdbTableAccessors.tableColumnAccessor.PKX.get(columnId);
    }

    @Override
    public LVTableColumn createNewColumn(LVTable table, String name, ColumnType type) throws IOException {
        assert (table.getTableId() > 0);
        if (name == null || name.length() == 0) {
            throw new IOException ("empty column name");
        }
        int maxOrder = -1;
        for (LVTableColumn column : bdbTableAccessors.tableColumnAccessor.IX_TABLE_ID.subIndex(table.getTableId()).map().values()) {
            if (column.getName().equalsIgnoreCase(name)) {
                throw new IOException ("this column name already exists: " + name);
            }
            maxOrder = Math.max(maxOrder, column.getOrder());
        }
        assert (maxOrder > 0);

        LVTableColumn column = new LVTableColumn();
        column.setName(name);
        column.setOrder(maxOrder + 1);
        column.setStatus(ColumnStatus.BEING_CREATED);
        column.setType(type);
        column.setTableId(table.getTableId());
        column.setColumnId(bdbTableAccessors.tableColumnAccessor.issueNewId());
        bdbTableAccessors.tableColumnAccessor.PKX.putNoReturn(column);
        return column;
    }

    @Override
    public void requestDropColumn(LVTableColumn column) throws IOException {
        assert (column.getColumnId() > 0);
        if (LOG.isInfoEnabled()) {
            LOG.info("drop column requested : " + column);
        }
        if (!bdbTableAccessors.tableColumnAccessor.PKX.contains(column.getColumnId())) {
            throw new IOException("this column does not exist. already dropped? : " + column);
        }
        column.setStatus(ColumnStatus.BEING_DROPPED);
        bdbTableAccessors.tableColumnAccessor.PKX.putNoReturn(column);
    }

    @Override
    public void dropColumn(LVTableColumn column) throws IOException {
        assert (column.getColumnId() > 0);
        boolean deleted = bdbTableAccessors.tableColumnAccessor.PKX.delete(column.getColumnId());
        if (!deleted) {
            LOG.warn("this column was already deleted?? :" + column);
        }
    }

    @Override
    public LVTableFracture getFracture(int fractureId) throws IOException {
        return bdbTableAccessors.tableFractureAccessor.PKX.get(fractureId);
    }

    @Override
    public LVTableFracture[] getAllFractures(int tableId) throws IOException {
        Collection<LVTableFracture> values = bdbTableAccessors.tableFractureAccessor.IX_TABLE_ID.subIndex(tableId).map().values();
        return values.toArray(new LVTableFracture[values.size()]);
    }

    @Override
    public LVTableFracture createNewFracture(LVTable table) throws IOException {
        assert (table.getTableId() > 0);
        LVTableFracture fracture = new LVTableFracture();
        fracture.setTableId(table.getTableId());
        fracture.setFractureId(bdbTableAccessors.tableFractureAccessor.issueNewId());
        bdbTableAccessors.tableFractureAccessor.PKX.putNoReturn(fracture);
        return fracture;
    }

    @Override
    public void finalizeFracture(LVTableFracture fracture) throws IOException {
        assert (fracture.getFractureId() > 0);
        bdbTableAccessors.tableFractureAccessor.PKX.putNoReturn(fracture);
    }

    @Override
    public void dropFracture(LVTableFracture fracture) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public LVReplicaGroup getReplicaGroup(int groupId) throws IOException {
        return bdbTableAccessors.replicaGroupAccessor.PKX.get(groupId);
    }

    @Override
    public LVReplicaGroup[] getAllReplicaGroups(int tableId) throws IOException {
        Collection<LVReplicaGroup> values = bdbTableAccessors.replicaGroupAccessor.IX_TABLE_ID.subIndex(tableId).map().values();
        return values.toArray(new LVReplicaGroup[values.size()]);
    }

    @Override
    public LVReplicaGroup getBaseReplicaGroup(int tableId) throws IOException {
        for(LVReplicaGroup group : bdbTableAccessors.replicaGroupAccessor.IX_TABLE_ID.subIndex(tableId).map().values()) {
            if (group.isBaseGroup()) {
                return group;
            }
        }
        throw new IOException ("Error! this tableId does not have base group. " + tableId);
    }

    @Override
    public LVReplicaGroup createNewReplicaGroup(LVTable table, LVTableColumn partitioningColumn) throws IOException {
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
        group.setBaseGroup(false);
        group.setPartitioningColumnId(partitioningColumn.getColumnId());
        group.setTableId(table.getTableId());
        group.setGroupId(bdbTableAccessors.replicaGroupAccessor.issueNewId());
        bdbTableAccessors.replicaGroupAccessor.PKX.putNoReturn(group);
        
        return group;
    }

    @Override
    public void dropReplicaGroup(LVReplicaGroup group) throws IOException {
        // TODO Auto-generated method stub        
    }

    @Override
    public LVReplicaScheme getReplicaScheme(int schemeId) throws IOException {
        return bdbTableAccessors.replicaSchemeAccessor.PKX.get(schemeId);
    }

    @Override
    public LVReplicaScheme[] getAllReplicaSchemes(int groupId) throws IOException {
        Collection<LVReplicaScheme> values = bdbTableAccessors.replicaSchemeAccessor.IX_GROUP_ID.subIndex(groupId).map().values();
        return values.toArray(new LVReplicaScheme[values.size()]);
    }

    @Override
    public LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVTableColumn sortingColumn, Map<Integer, CompressionType> columnCompressionSchemes)
                    throws IOException {
        assert (group.getGroupId() > 0);
        assert (sortingColumn.getColumnId() > 0);
        assert (group.getTableId() == sortingColumn.getTableId());
        
        HashMap<Integer, CompressionType> clonedCompressionSchemes = new HashMap<Integer, CompressionType> (columnCompressionSchemes);
        boolean foundSortingColumn = false;
        // complement compression type
        for (LVTableColumn column : getAllColumns(group.getTableId())) {
            if (column.getColumnId() == sortingColumn.getColumnId()) {
                foundSortingColumn = true;
            }
            if (!clonedCompressionSchemes.containsKey(column.getColumnId())) {
                if (column.getName().equals(LVTableColumn.EPOCH_COLUMN_NAME)) {
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
    public LVReplicaScheme changeColumnCompressionScheme(LVReplicaScheme scheme, LVTableColumn column, CompressionType compressionType) throws IOException {
        assert (scheme.getSchemeId() > 0);
        scheme.getColumnCompressionSchemes().put(column.getColumnId(), compressionType);
        bdbTableAccessors.replicaSchemeAccessor.PKX.putNoReturn(scheme);
        return scheme;
    }

    @Override
    public void dropReplicaScheme(LVReplicaScheme scheme) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVReplica getReplica(int replicaId) throws IOException {
        return bdbTableAccessors.replicaAccessor.PKX.get(replicaId);
    }

    @Override
    public LVReplica[] getAllReplicasBySchemeId(int schemeId) throws IOException {
        Collection<LVReplica> values = bdbTableAccessors.replicaAccessor.IX_SCHEME_ID.subIndex(schemeId).map().values();
        return values.toArray(new LVReplica[values.size()]);
    }

    @Override
    public LVReplica[] getAllReplicasByFractureId(int fractureId) throws IOException {
        Collection<LVReplica> values = bdbTableAccessors.replicaAccessor.IX_FRACTURE_ID.subIndex(fractureId).map().values();
        return values.toArray(new LVReplica[values.size()]);
    }

    @Override
    public LVReplica getReplicaFromSchemeAndFracture(int schemeId, int fractureId) throws IOException {
        return bdbTableAccessors.replicaAccessor.IX_SCHEME_FRACTURE_ID.get(new CompositeIntKey(schemeId, fractureId));
    }

    @Override
    public LVReplica createNewReplica(LVReplicaScheme scheme, LVTableFracture fracture) throws IOException {
        assert (scheme.getSchemeId() > 0);
        assert (fracture.getFractureId() > 0);
        LVReplica replica = new LVReplica();
        replica.setFractureId(fracture.getFractureId());
        replica.setSchemeId(scheme.getSchemeId());
        replica.setStatus(ReplicaStatus.OK);
        replica.setReplicaId(bdbTableAccessors.replicaAccessor.issueNewId());
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
        // TODO Auto-generated method stub
    }

    @Override
    public LVReplicaPartitionScheme getReplicaPartitionScheme(int subPartitionSchemeId) throws IOException {
        return bdbTableAccessors.replicaPartitionSchemeAccessor.PKX.get(subPartitionSchemeId);
    }

    @Override
    public LVReplicaPartitionScheme[] getAllReplicaPartitionSchemesByFractureId(int fractureId) throws IOException {
        Collection<LVReplicaPartitionScheme> values = bdbTableAccessors.replicaPartitionSchemeAccessor.IX_FRACTURE_ID.subIndex(fractureId).map().values();
        return values.toArray(new LVReplicaPartitionScheme[values.size()]);
    }

    @Override
    public LVReplicaPartitionScheme[] getAllReplicaPartitionSchemesByGroupId(int groupId) throws IOException {
        Collection<LVReplicaPartitionScheme> values = bdbTableAccessors.replicaPartitionSchemeAccessor.IX_GROUP_ID.subIndex(groupId).map().values();
        return values.toArray(new LVReplicaPartitionScheme[values.size()]);
    }

    @Override
    public LVReplicaPartitionScheme getReplicaPartitionSchemeByFractureAndGroup(int fractureId, int groupId) throws IOException {
        return bdbTableAccessors.replicaPartitionSchemeAccessor.IX_FRACTURE_GROUP_ID.get(new CompositeIntKey(fractureId, groupId));
    }

    @Override
    public LVReplicaPartitionScheme createNewReplicaPartitionScheme(LVTableFracture fracture, LVReplicaGroup group) throws IOException {
        assert (fracture.getFractureId() > 0);
        assert (group.getGroupId() > 0);
        assert (fracture.getTableId() == group.getTableId());
        LVReplicaPartitionScheme partitionScheme = new LVReplicaPartitionScheme();
        partitionScheme.setFractureId(fracture.getFractureId());
        partitionScheme.setGroupId(group.getGroupId());
        partitionScheme.setReplicaPartitionSchemeId(bdbTableAccessors.replicaPartitionSchemeAccessor.issueNewId());
        bdbTableAccessors.replicaPartitionSchemeAccessor.PKX.putNoReturn(partitionScheme);
        return partitionScheme;
    }

    @Override
    public void finalizeReplicaPartitionScheme(LVReplicaPartitionScheme subPartitionScheme) throws IOException {
        assert (subPartitionScheme.getReplicaPartitionSchemeId() > 0);
        bdbTableAccessors.replicaPartitionSchemeAccessor.PKX.putNoReturn(subPartitionScheme);
    }

    @Override
    public void dropReplicaPartitionScheme(LVReplicaPartitionScheme subPartitionScheme) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public LVReplicaPartition getReplicaPartition(int subPartitionId) throws IOException {
        return bdbTableAccessors.replicaPartitionAccessor.PKX.get(subPartitionId);
    }

    @Override
    public LVReplicaPartition[] getAllReplicaPartitionsByReplicaId(int replicaId) throws IOException {
        Collection<LVReplicaPartition> values = bdbTableAccessors.replicaPartitionAccessor.IX_REPLICA_ID.subIndex(replicaId).map().values();
        return values.toArray(new LVReplicaPartition[values.size()]);
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
        LVReplicaScheme scheme = getReplicaScheme(replica.getSchemeId());
        LVReplicaPartitionScheme subPartitionScheme = getReplicaPartitionSchemeByFractureAndGroup(replica.getFractureId(), scheme.getGroupId());
        subPartition.setReplicaPartitionSchemeId(subPartitionScheme.getReplicaPartitionSchemeId());

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
        // TODO Auto-generated method stub        
    }

    @Override
    public LVColumnFile getColumnFile(int columnFileId) throws IOException {
        return bdbTableAccessors.columnFileAccessor.PKX.get(columnFileId);
    }

    @Override
    public LVColumnFile[] getAllColumnFilesByReplicaPartitionId(int subPartitionId) throws IOException {
        Collection<LVColumnFile> values = bdbTableAccessors.columnFileAccessor.IX_PARTITION_ID.subIndex(subPartitionId).map().values();
        return values.toArray(new LVColumnFile[values.size()]);
    }

    @Override
    public LVColumnFile getColumnFileByReplicaPartitionAndColumn(int subPartitionId, int columnId) throws IOException {
        return bdbTableAccessors.columnFileAccessor.IX_PARTITION_COLUMN_ID.get(new CompositeIntKey(subPartitionId, columnId));
    }

    @Override
    public LVColumnFile createNewColumnFile(LVReplicaPartition subPartition, LVTableColumn column, String hdfsFilePath, long fileSize) throws IOException {
        assert (column.getColumnId() > 0);
        assert (subPartition.getPartitionId() > 0);
        LVColumnFile file = new LVColumnFile();
        file.setColumnFileId(bdbTableAccessors.columnFileAccessor.issueNewId());
        file.setColumnId(column.getColumnId());
        file.setFileSize(fileSize);
        file.setHdfsFilePath(hdfsFilePath);
        file.setPartitionId(subPartition.getPartitionId());
        bdbTableAccessors.columnFileAccessor.PKX.putNoReturn(file);
        return file;
    }

    @Override
    public void dropColumnFile(LVColumnFile columnFile) throws IOException {
        assert (columnFile.getColumnFileId() > 0);
        boolean deleted = bdbTableAccessors.columnFileAccessor.PKX.delete(columnFile.getColumnFileId());
        if (!deleted) {
            LOG.warn("this column file was already deleted?? :" + columnFile);
        }
    }
}
