package edu.brown.lasvegas.lvfs.meta;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.log4j.Logger;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityIndex;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

import edu.brown.lasvegas.ColumnStatus;
import edu.brown.lasvegas.DatabaseStatus;
import edu.brown.lasvegas.FractureStatus;
import edu.brown.lasvegas.JobStatus;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVDatabase;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.LVObject;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackAssignment;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVTask;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.RackStatus;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.ReplicaStatus;
import edu.brown.lasvegas.TableStatus;
import edu.brown.lasvegas.TaskStatus;
import edu.brown.lasvegas.TaskType;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;
import edu.brown.lasvegas.util.CompositeIntKey;
import edu.brown.lasvegas.util.ValueRange;

/**
 * Implementation of {@link LVMetadataProtocol} in the master namenode.
 * This can directly handle all read and write accesses over the local BDB-JE
 * instance.
 */
public class MasterMetadataRepository implements LVMetadataProtocol {
    private static Logger LOG = Logger.getLogger(MasterMetadataRepository.class);
    
    private final File bdbEnvHome;
    private final EnvironmentConfig bdbEnvConf;    
    private final Environment bdbEnv;
    
    private BdbTableAccessors bdbTableAccessors;
    private boolean didShutdown = false;
    
    private static final long BDB_CACHE_SIZE = 1L << 26;
    
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        if (protocol.equals(LVMetadataProtocol.class.getName())) {
            return LVMetadataProtocol.versionID;
        } else {
            throw new IOException("This protocol is not supported: " + protocol);
        }
    }
    
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

        // metedata repository is accessed from multi-threads. so, it has to be transactional
        bdbEnvConf.setLocking(true);
        bdbEnvConf.setTransactional(true);
        // however, we don't need 100% durability.(arguably..)
        bdbEnvConf.setDurability(Durability.COMMIT_NO_SYNC);

        bdbEnv = new Environment(bdbEnvHome, bdbEnvConf);

        loadRepository();
    }
    
    /** rename the BDB home to cleanup everything. */
    private void formatRepository () throws IOException {
        // we never delete the old repository. just rename.
        File backup = new File(bdbEnvHome.getParentFile(), bdbEnvHome.getName() + "_backup_"
            + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) // append backup-date
            + "_" + Math.abs(new Random(System.nanoTime()).nextInt())); // to make it unique
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
        return bdbTableAccessors.masterTableAccessor.issueNewIdBlock(MasterTable.EPOCH_SEQ, 1);
    }

    @Override
    public int issueNewId(int objectTypeOrdinal) throws IOException {
        return bdbTableAccessors.masterTableAccessor.issueNewId(objectTypeOrdinal);
    }
    @Override
    public int issueNewIdBlock(int objectTypeOrdinal, int blockSize) throws IOException {
        if (blockSize <= 0) {
            throw new IOException ("invalid blockSize:" + blockSize);
        }
        return bdbTableAccessors.masterTableAccessor.issueNewIdBlock(objectTypeOrdinal, blockSize);
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
    public void shutdown () throws IOException {
        LOG.info("shutting down...");
        sync();
        bdbTableAccessors.closeAll();
        bdbEnv.close();
        didShutdown = true;
        LOG.info("shutdown.");
    }
    public boolean isShutdown () {
        return didShutdown;
    }

    @Override
    public LVDatabase getDatabase(int databaseId) throws IOException {
        return getTransactional(bdbTableAccessors.databaseAccessor.PKX, databaseId);
    }
    @Override
    public LVDatabase getDatabase(String name) throws IOException {
        return getTransactional(bdbTableAccessors.databaseAccessor.IX_NAME, name);
    }
    @Override
    public LVDatabase[] getAllDatabases() throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.databaseAccessor.PKX).toArray(new LVDatabase[0]);
    }
    @Override
    public LVDatabase createNewDatabase(String name) throws IOException {
        LOG.info("creating new database " + name + "...");
        if (name == null || name.length() == 0) {
            throw new IOException ("empty database name");
        }
        if (bdbTableAccessors.databaseAccessor.IX_NAME.contains(name)) {
            throw new IOException ("this database name already exists:" + name);
        }
        final LVDatabase database = new LVDatabase();
        database.setDatabaseId(bdbTableAccessors.databaseAccessor.issueNewId());
        database.setName(name);
        database.setStatus(DatabaseStatus.OK);
        putNoReturnTransactional (bdbTableAccessors.databaseAccessor.PKX, database);
        return database;
    }
    @Override
    public void requestDropDatabase(int databaseId) throws IOException {
        final LVDatabase database = getDatabase(databaseId);
        if (database == null) {
            throw new IOException("this databaseId does not exist. already dropped? : " + databaseId);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("drop database requested : " + database);
        }
        database.setStatus(DatabaseStatus.BEING_DROPPED);
        putNoReturnTransactional (bdbTableAccessors.databaseAccessor.PKX, database);
    }
    @Override
    public void dropDatabase(final int databaseId) throws IOException {
        final LVDatabase database = getDatabase(databaseId);
        if (database == null) {
            throw new IOException("this databaseId does not exist. already dropped? : " + databaseId);
        }

        // drop tables
        LVTable[] tables = getAllTables(databaseId);
        for (LVTable table : tables) {
            dropTable(table);
        }
        boolean deleted = deleteTransactional (bdbTableAccessors.databaseAccessor.PKX, databaseId);
        if (!deleted) {
            LOG.warn("this database has been already deleted?? :" + database);
        }
        LOG.info("Dropped");
    }
    
    @Override
    public LVTable getTable(int tableId) throws IOException {
        return getTransactional(bdbTableAccessors.tableAccessor.PKX, tableId);
    }
    @Override
    public LVTable getTable(int databaseId, String name) throws IOException {
        for (LVTable table : fetchAll(bdbTableAccessors.tableAccessor.IX_NAME, name)) {
            if (table.getDatabaseId() == databaseId) {
                assert (table.getName().equalsIgnoreCase(name));
                return table;
            }
        }
        return null;
    }
    @Override
    public LVTable[] getAllTables(int databaseId) throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.tableAccessor.IX_DATABASE_ID, databaseId).toArray(new LVTable[0]);
    }

    @Override
    public LVTable createNewTable(int databaseId, String name, String[] columnNames, ColumnType[] columnTypes) throws IOException {
        return createNewTable(databaseId, name, columnNames, columnTypes, -1);
    }
    @Override
    public LVTable createNewTable(int databaseId, String name, String[] columnNames, ColumnType[] columnTypes, int fracturingColumn) throws IOException {
        LOG.info("creating new table " + name + " in databaseId=" + databaseId + "...");
        // misc parameter check
        if (name == null || name.length() == 0) {
            throw new IOException ("empty table name");
        }
        if (columnNames.length == 0) {
            throw new IOException ("table without any columns is not allowed");
        }
        if (columnNames.length != columnTypes.length) {
            throw new IOException ("the size of columnNames doesn't agree with that of columnTypes");
        }
        if (fracturingColumn < -1 || fracturingColumn >= columnNames.length) {
            throw new IOException ("invalid fracturingColumn:" + fracturingColumn);
        }
        {
            LVTable existingTable = getTable(databaseId, name);
            if (existingTable != null) {
                throw new IOException ("this table name already exists in this database:" + name);
            }
        }

        {
            // check column name duplicates
            HashSet<String> columnNameSet = new HashSet<String>();
            columnNameSet.add(LVColumn.EPOCH_COLUMN_NAME);
            for (String columnName : columnNames) {
                if (columnName == null || columnName.length() == 0) {
                    throw new IOException ("empty column name");
                }
                if (columnNameSet.contains(columnName.toLowerCase())) {
                    throw new IOException ("this column name is used more than once:" + columnName);
                }
                columnNameSet.add(columnName.toLowerCase());
            }
        }

        // first, create table record
        final LVTable table = new LVTable();
        table.setDatabaseId(databaseId);
        table.setName(name);
        table.setStatus(TableStatus.BEING_CREATED);
        int tableId = bdbTableAccessors.tableAccessor.issueNewId();
        table.setTableId(tableId);
        table.setFracturingColumnId(-1); // we don't know this at this point
        putNoReturnTransactional (bdbTableAccessors.tableAccessor.PKX, table);
        
        // then, create columns. first column is the implicit epoch column
        int fracturingColumnId = -1;
        {
            final LVColumn column = new LVColumn();
            column.setName(LVColumn.EPOCH_COLUMN_NAME);
            column.setOrder(0);
            column.setStatus(ColumnStatus.OK);
            column.setType(ColumnType.INTEGER);
            column.setTableId(tableId);
            int columnId = bdbTableAccessors.columnAccessor.issueNewId();
            column.setColumnId(columnId);
            column.setFracturingColumn(fracturingColumn == -1);
            putNoReturnTransactional (bdbTableAccessors.columnAccessor.PKX, column);
            if (column.isFracturingColumn()) {
                fracturingColumnId = columnId;
            }
        }
        for (int i = 0; i < columnNames.length; ++i) {
            int order = i + 1;
            final LVColumn column = new LVColumn();
            column.setName(columnNames[i]);
            column.setOrder(order);
            column.setStatus(ColumnStatus.OK);
            column.setType(columnTypes[i]);
            column.setTableId(tableId);
            int columnId = bdbTableAccessors.columnAccessor.issueNewId();
            column.setColumnId(columnId);
            column.setFracturingColumn(fracturingColumn == i);
            putNoReturnTransactional (bdbTableAccessors.columnAccessor.PKX, column);
            if (column.isFracturingColumn()) {
                fracturingColumnId = columnId;
            }
        }
        assert (fracturingColumnId != -1);
        
        // finally,  update the table
        table.setFracturingColumnId(fracturingColumnId);
        table.setStatus(TableStatus.OK);
        putNoReturnTransactional (bdbTableAccessors.tableAccessor.PKX, table);
        
        LOG.info("created new table");
        return table;
    }

    @Override
    public void requestDropTable(final LVTable table) throws IOException {
        assert (table.getTableId() > 0);
        if (LOG.isInfoEnabled()) {
            LOG.info("drop table requested : " + table);
        }
        if (!bdbTableAccessors.tableAccessor.PKX.contains(table.getTableId())) {
            throw new IOException("this table does not exist. already dropped? : " + table);
        }
        table.setStatus(TableStatus.BEING_DROPPED);
        putNoReturnTransactional (bdbTableAccessors.tableAccessor.PKX, table);
    }

    @Override
    public void dropTable(final LVTable table) throws IOException {
        LOG.info("Dropping table : " + table);
        assert (table.getTableId() > 0);
        // drop child fractures
        LVFracture[] fractures = getAllFractures(table.getTableId());
        for (LVFracture fracture : fractures) {
            dropFracture(fracture.getFractureId());
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
        boolean deleted = deleteTransactional (bdbTableAccessors.tableAccessor.PKX, table.getTableId());
        if (!deleted) {
            LOG.warn("this table has been already deleted?? :" + table);
        }
        LOG.info("Dropped");
    }

    @Override
    public LVColumn[] getAllColumns(int tableId) throws IOException {
        Collection<LVColumn> entities = fetchAll(bdbTableAccessors.columnAccessor.IX_TABLE_ID, tableId);
        SortedMap<Integer, LVColumn> orderedMap = new TreeMap<Integer, LVColumn>(); // sorted by Order
        for (LVColumn column : entities) {
            assert (!orderedMap.containsKey(column.getOrder()));
            orderedMap.put(column.getOrder(), column);
        }
        return orderedMap.values().toArray(new LVColumn[orderedMap.size()]);
    }
    @Override
    public LVColumn[] getAllColumnsExceptEpochColumn(int tableId) throws IOException {
        LVColumn[] allColumns = getAllColumns(tableId);
        assert (allColumns[0].getName().equals(LVColumn.EPOCH_COLUMN_NAME));
        LVColumn[] ret = new LVColumn[allColumns.length - 1];
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = allColumns[i + 1];
        }
        return ret;
    }

    @Override
    public LVColumn getColumn(int columnId) throws IOException {
        return getTransactional(bdbTableAccessors.columnAccessor.PKX, columnId);
    }
    @Override
    public LVColumn getColumnByName(int tableId, String name) throws IOException {
        for (LVColumn column : fetchAll(bdbTableAccessors.columnAccessor.IX_TABLE_ID, tableId)) {
            if (name.equals(column.getName())) {
                return column;
            }
        }
        return null;
    }

    @Override
    public LVColumn createNewColumn(LVTable table, String name, ColumnType type) throws IOException {
        assert (table.getTableId() > 0);
        if (name == null || name.length() == 0) {
            throw new IOException ("empty column name");
        }
        int maxOrder = -1;
        for (LVColumn column : fetchAll(bdbTableAccessors.columnAccessor.IX_TABLE_ID, table.getTableId())) {
            if (column.getName().equalsIgnoreCase(name)) {
                throw new IOException ("this column name already exists: " + name);
            }
            maxOrder = Math.max(maxOrder, column.getOrder());
        }
        assert (maxOrder > 0);

        final LVColumn column = new LVColumn();
        column.setName(name);
        column.setOrder(maxOrder + 1);
        column.setStatus(ColumnStatus.BEING_CREATED);
        column.setType(type);
        column.setTableId(table.getTableId());
        column.setFracturingColumn(false);
        column.setColumnId(bdbTableAccessors.columnAccessor.issueNewId());
        putNoReturnTransactional(bdbTableAccessors.columnAccessor.PKX, column);
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
        putNoReturnTransactional(bdbTableAccessors.columnAccessor.PKX, column);
    }

    @Override
    public void dropColumn(LVColumn column) throws IOException {
        assert (column.getColumnId() > 0);
        // check if this column can't be deleted
        for (LVReplicaGroup group : getAllReplicaGroups(column.getTableId())) { 
            if (group.getPartitioningColumnId() == column.getColumnId()) {
                throw new IOException ("partitioning column can't be deleted. drop this replica group first:" + group);
            }
            for (LVReplicaScheme scheme : getAllReplicaSchemes(group.getGroupId())) {
                if (scheme.getSortColumnId() != null && scheme.getSortColumnId().intValue() == column.getColumnId()) {
                    throw new IOException ("sorting column can't be deleted. drop this replica scheme first:" + scheme);
                }
            }
        }
        // delete column files
        for (LVColumnFile file : fetchAll(bdbTableAccessors.columnFileAccessor.IX_COLUMN_ID, column.getColumnId())) {
            dropColumnFile(file.getColumnFileId());
        }
        
        // then delete the column
        boolean deleted = deleteTransactional(bdbTableAccessors.columnAccessor.PKX, column.getColumnId());
        if (!deleted) {
            LOG.warn("this column has been already deleted?? :" + column);
        }
    }

    @Override
    public LVFracture getFracture(int fractureId) throws IOException {
        return getTransactional(bdbTableAccessors.fractureAccessor.PKX, fractureId);
    }

    @Override
    public LVFracture[] getAllFractures(int tableId) throws IOException {
        // ID order
        Collection<LVFracture> values = fetchAll(bdbTableAccessors.fractureAccessor.IX_TABLE_ID, tableId);
        return values.toArray(new LVFracture[values.size()]);
    }

    @Override
    public LVFracture createNewFracture(LVTable table) throws IOException {
        assert (table != null);
        LVFracture fracture = new LVFracture();
        fracture.setTableId(table.getTableId());
        fracture.setFractureId(bdbTableAccessors.fractureAccessor.issueNewId());
        fracture.setStatus(FractureStatus.INACTIVE);
        fracture.setTupleCount(0);
        LVColumn fracturingColumn = getColumn(table.getFracturingColumnId());
        fracture.setRange(new ValueRange(fracturingColumn.getType(), null, null));
        putNoReturnTransactional(bdbTableAccessors.fractureAccessor.PKX, fracture);
        return fracture;
    }
    @Override
    public int createNewFractureIdOnlyReturn(int tableId) throws IOException {
        return createNewFracture (getTable(tableId)).getFractureId();
    }

    @Override
    public LVFracture updateFracture(int fractureId, FractureStatus status, LongWritable tupleCount, ValueRange range) throws IOException {
        LVFracture fracture = getFracture(fractureId);
        assert (fracture != null);
        if (status != null && status != FractureStatus.INVALID) {
            fracture.setStatus(status);
        }
        if (tupleCount != null) {
            fracture.setTupleCount(tupleCount.get());
        }
        if (range != null) {
            fracture.setRange(range);
        }
        putNoReturnTransactional(bdbTableAccessors.fractureAccessor.PKX, fracture);
        return fracture;
    }
    @Override
    public void updateFractureNoReturn(int fractureId, FractureStatus status, LongWritable tupleCount, ValueRange range) throws IOException {
        updateFracture(fractureId, status, tupleCount, range);
    }

    @Override
    public void dropFracture(int fractureId) throws IOException {
        LOG.info("Dropping fracture : " + fractureId);
        assert (fractureId > 0);
        // drop child replicas
        LVReplica[] replicas = getAllReplicasByFractureId(fractureId);
        for (LVReplica replica : replicas) {
            dropReplica(replica);
        }
        // drop rack assignments
        for (LVRackAssignment assignment : getAllRackAssignmentsByFractureId(fractureId)) {
            dropRackAssignment(assignment);
        }
        boolean deleted = deleteTransactional(bdbTableAccessors.fractureAccessor.PKX, fractureId);
        if (!deleted) {
            LOG.warn("this fracture has been already deleted?? :" + fractureId);
        }
        LOG.info("Dropped");
    }

    @Override
    public LVReplicaGroup getReplicaGroup(int groupId) throws IOException {
        return getTransactional(bdbTableAccessors.replicaGroupAccessor.PKX, groupId);
    }

    @Override
    public LVReplicaGroup[] getAllReplicaGroups(int tableId) throws IOException {
        // ID order
        Collection<LVReplicaGroup> values = fetchAll(bdbTableAccessors.replicaGroupAccessor.IX_TABLE_ID, tableId);
        return values.toArray(new LVReplicaGroup[values.size()]);
    }

    @Override
    public LVReplicaGroup createNewReplicaGroup(LVTable table) throws IOException {
        return createNewReplicaGroup(table, null, null, null);
    }
    @Override
    public LVReplicaGroup createNewReplicaGroup(LVTable table, LVColumn partitioningColumn, ValueRange[] ranges) throws IOException {
        return createNewReplicaGroup(table, partitioningColumn, ranges, null);
    }
    @Override
    public LVReplicaGroup createNewReplicaGroup(LVTable table, LVColumn partitioningColumn, LVReplicaGroup linkedGroup) throws IOException {
        return createNewReplicaGroup(table, partitioningColumn, null, linkedGroup);
    }
    private LVReplicaGroup createNewReplicaGroup(LVTable table, LVColumn partitioningColumn, ValueRange[] ranges, LVReplicaGroup linkedGroup) throws IOException {
        assert (table.getTableId() > 0);
        assert (partitioningColumn == null || partitioningColumn.getColumnId() > 0);
        assert (partitioningColumn == null || table.getTableId() == partitioningColumn.getTableId());
        assert (linkedGroup == null || linkedGroup.getGroupId() > 0);
        assert (partitioningColumn == null || linkedGroup != null || ranges != null);
        // check other group
        if (partitioningColumn != null) {
            for (LVReplicaGroup existing : fetchAll(bdbTableAccessors.replicaGroupAccessor.IX_TABLE_ID, table.getTableId())) {
                assert (table.getTableId() == existing.getTableId());
                if (existing.getPartitioningColumnId() != null && existing.getPartitioningColumnId() == partitioningColumn.getColumnId()) {
                    throw new IOException ("another replica group with the same partitioning column already exists : " + existing);
                }
            }
        } else {
            // if no partitioning, there is a single partition spanning all ranges (this makes sure #partitions>0 always)
            ranges = new ValueRange[]{new ValueRange(ColumnType.INVALID, null, null)};
        }
        // if linked group is specified, check partitioning column type
        if (linkedGroup != null) {
            // check column type
            LVColumn linkedColumn = getColumn(linkedGroup.getPartitioningColumnId());
            assert (linkedColumn != null);
            if (linkedColumn.getType() != partitioningColumn.getType()) {
                throw new IOException("linked group must be partitioned by a column in the same type: linked=" + linkedColumn.getType() + ", this=" + partitioningColumn.getType());
            }
            ranges = linkedGroup.getRanges();
        }
        // check range consistency.
        assert (ranges != null);
        if (ranges.length == 0) {
            throw new IOException ("no partitioning ranges defined");
        }
        if (ranges[0].getStartKey() != null) {
            throw new IOException ("the start-key of the first range must be null");
        }
        if (ranges.length != 1 && ranges[0].getEndKey() == null) {
            throw new IOException ("the end-key of range 0 is null");
        }
        Comparable<?> prevEnd = ranges[0].getEndKey();
        for (int i = 1; i < ranges.length; ++i) {
            if (ranges[i].getStartKey() == null) {
                throw new IOException ("the start-key of range " + i + " is null");
            }
            if (!prevEnd.equals(ranges[i].getStartKey())) {
                throw new IOException ("the end-key of range " + (i - 1) + " doesn't match with the start-key of range " + i);
            }
            if (i != ranges.length - 1 && ranges[i].getEndKey() == null) {
                throw new IOException ("the end-key of range " + i + " is null");
            }
            prevEnd = ranges[i].getEndKey();
        }
        if (ranges[ranges.length - 1].getEndKey() != null) {
            throw new IOException ("the end-key of the last range must be null");
        }
        
        LVReplicaGroup group = new LVReplicaGroup();
        group.setPartitioningColumnId(partitioningColumn == null ? null : partitioningColumn.getColumnId());
        group.setRanges(ranges);
        group.setTableId(table.getTableId());
        group.setGroupId(bdbTableAccessors.replicaGroupAccessor.issueNewId());
        group.setLinkedGroupId(linkedGroup == null ? null : linkedGroup.getGroupId());
        putNoReturnTransactional(bdbTableAccessors.replicaGroupAccessor.PKX, group);
        
        return group;
    }

    @Override
    public void dropReplicaGroup(LVReplicaGroup group) throws IOException {
        LOG.info("Dropping replica group : " + group);
        assert (group.getGroupId() > 0);
        // drop child replica schemes
        LVReplicaScheme[] schemes = getAllReplicaSchemes(group.getGroupId());
        for (LVReplicaScheme scheme : schemes) {
            dropReplicaScheme(scheme);
        }
        boolean deleted = deleteTransactional(bdbTableAccessors.replicaGroupAccessor.PKX, group.getGroupId());
        if (!deleted) {
            LOG.warn("this replica group has been already deleted?? :" + group);
        }
        LOG.info("Dropped");
    }

    @Override
    public LVReplicaScheme getReplicaScheme(int schemeId) throws IOException {
        return getTransactional(bdbTableAccessors.replicaSchemeAccessor.PKX, schemeId);
    }

    @Override
    public LVReplicaScheme[] getAllReplicaSchemes(int groupId) throws IOException {
        // ID order
        Collection<LVReplicaScheme> values = fetchAll(bdbTableAccessors.replicaSchemeAccessor.IX_GROUP_ID, groupId);
        return values.toArray(new LVReplicaScheme[values.size()]);
    }

    @Override
    public LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVColumn sortingColumn, int[] columnIds, CompressionType[] columnCompressionSchemes)
                    throws IOException {
        assert (group.getGroupId() > 0);
        assert (sortingColumn == null || sortingColumn.getColumnId() > 0);
        assert (sortingColumn == null || group.getTableId() == sortingColumn.getTableId());
        assert (columnIds.length == columnCompressionSchemes.length);
        
        HashMap<Integer, CompressionType> clonedCompressionSchemes = new HashMap<Integer, CompressionType> ();
        for (int i = 0; i < columnIds.length; ++i) {
            clonedCompressionSchemes.put (columnIds[i], columnCompressionSchemes[i]);
        }
        boolean foundSortingColumn = false;
        // complement compression type
        for (LVColumn column : getAllColumns(group.getTableId())) {
            if (sortingColumn != null && column.getColumnId() == sortingColumn.getColumnId()) {
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
        assert (sortingColumn == null || foundSortingColumn);

        LVReplicaScheme scheme = new LVReplicaScheme();
        scheme.setColumnCompressionSchemes(clonedCompressionSchemes);
        scheme.setGroupId(group.getGroupId());
        scheme.setSchemeId(bdbTableAccessors.replicaSchemeAccessor.issueNewId());
        scheme.setSortColumnId(sortingColumn == null ? null : sortingColumn.getColumnId());
        putNoReturnTransactional(bdbTableAccessors.replicaSchemeAccessor.PKX, scheme);
        return scheme;
    }

    @Override
    public LVReplicaScheme changeColumnCompressionScheme(LVReplicaScheme scheme, LVColumn column, CompressionType compressionType) throws IOException {
        assert (scheme.getSchemeId() > 0);
        scheme.getColumnCompressionSchemes().put(column.getColumnId(), compressionType);
        putNoReturnTransactional(bdbTableAccessors.replicaSchemeAccessor.PKX, scheme);
        return scheme;
    }

    @Override
    public void dropReplicaScheme(LVReplicaScheme scheme) throws IOException {
        assert (scheme.getSchemeId() > 0);
        LVReplica[] replicas = getAllReplicasBySchemeId(scheme.getSchemeId());
        for (LVReplica replica : replicas) {
            dropReplica(replica);
        }
        boolean deleted = deleteTransactional(bdbTableAccessors.replicaSchemeAccessor.PKX, scheme.getSchemeId());
        if (!deleted) {
            LOG.warn("this replica scheme has been already deleted?? :" + scheme);
        }
    }

    @Override
    public LVReplica getReplica(int replicaId) throws IOException {
        return getTransactional(bdbTableAccessors.replicaAccessor.PKX, replicaId);
    }

    @Override
    public LVReplica[] getAllReplicasBySchemeId(int schemeId) throws IOException {
        // ID order
        Collection<LVReplica> values = fetchAll(bdbTableAccessors.replicaAccessor.IX_SCHEME_ID, schemeId);
        return values.toArray(new LVReplica[values.size()]);
    }

    @Override
    public LVReplica[] getAllReplicasByFractureId(int fractureId) throws IOException {
        // ID order
        Collection<LVReplica> values = fetchAll(bdbTableAccessors.replicaAccessor.IX_FRACTURE_ID, fractureId);
        return values.toArray(new LVReplica[values.size()]);
    }

    @Override
    public LVReplica getReplicaFromSchemeAndFracture(int schemeId, int fractureId) throws IOException {
        // fracture ID should be enough selective
        for (LVReplica replica : fetchAll(bdbTableAccessors.replicaAccessor.IX_FRACTURE_ID, fractureId)) {
            if (replica.getSchemeId() == schemeId) {
                return replica;
            }
        }
        return null;
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
        putNoReturnTransactional(bdbTableAccessors.replicaAccessor.PKX, replica);
        return replica;
    }

    @Override
    public LVReplica updateReplicaStatus(LVReplica replica, ReplicaStatus status) throws IOException {
        replica.setStatus(status);
        putNoReturnTransactional(bdbTableAccessors.replicaAccessor.PKX, replica);
        return replica;
    }

    @Override
    public void dropReplica(LVReplica replica) throws IOException {
        assert (replica.getReplicaId() > 0);
        LVReplicaPartition[] subPartitions = getAllReplicaPartitionsByReplicaId(replica.getReplicaId());
        for (LVReplicaPartition subPartition : subPartitions) {
            dropReplicaPartition(subPartition);
        }
        boolean deleted = deleteTransactional(bdbTableAccessors.replicaAccessor.PKX, replica.getReplicaId());
        if (!deleted) {
            LOG.warn("this replica has been already deleted?? :" + replica);
        }
    }
    @Override
    public LVReplicaPartition getReplicaPartition(int subPartitionId) throws IOException {
        return getTransactional(bdbTableAccessors.replicaPartitionAccessor.PKX, subPartitionId);
    }

    @Override
    public LVReplicaPartition[] getAllReplicaPartitionsByReplicaId(int replicaId) throws IOException {
        Collection<LVReplicaPartition> values = fetchAll(bdbTableAccessors.replicaPartitionAccessor.IX_REPLICA_ID, replicaId);
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
        return getTransactional(bdbTableAccessors.replicaPartitionAccessor.IX_REPLICA_RANGE, new CompositeIntKey(replicaId, range));
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
        // also sets ReplicaGroupId. this is a de-normalization
        LVReplicaScheme scheme = getReplicaScheme(replica.getSchemeId());
        subPartition.setReplicaGroupId(scheme.getGroupId());

        putNoReturnTransactional(bdbTableAccessors.replicaPartitionAccessor.PKX, subPartition);
        return subPartition;
    }

    @Override
    public LVReplicaPartition updateReplicaPartition(LVReplicaPartition subPartition, ReplicaPartitionStatus status,
                    LVRackNode node) throws IOException {
        assert (subPartition.getPartitionId() > 0);
        if (status == ReplicaPartitionStatus.OK && node == null) {
            throw new IOException ("a valid replica partition needs to be phisycally stored somewhere.");
        }
        subPartition.setStatus(status);
        if (node == null) {
            subPartition.setNodeId(null);
        } else {
            subPartition.setNodeId(node.getNodeId());
        }
        putNoReturnTransactional(bdbTableAccessors.replicaPartitionAccessor.PKX, subPartition);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updated ReplicaPartition:" + subPartition);
        }
        return subPartition;
    }
    @Override
    public void updateReplicaPartitionNoReturn(int subPartitionId, ReplicaPartitionStatus status, IntWritable nodeId) throws IOException {
        LVReplicaPartition subPartition = getReplicaPartition(subPartitionId);
        if (subPartition == null) {
            throw new IOException ("this replica partition ID doesn't exist: " + subPartitionId);
        }
        
        LVRackNode node = null;
        if (nodeId != null) {
            node = getRackNode(nodeId.get());
            if (node == null) {
                throw new IOException ("this node ID doesn't exist: " + nodeId);
            }
        }
        updateReplicaPartition (subPartition, status, node);
    }    

    @Override
    public void dropReplicaPartition(LVReplicaPartition subPartition) throws IOException {
        assert (subPartition.getPartitionId() > 0);
        LVColumnFile[] columnFiles = getAllColumnFilesByReplicaPartitionId (subPartition.getPartitionId());
        for (LVColumnFile columnFile : columnFiles) {
            dropColumnFile(columnFile.getColumnFileId());
        }
        boolean deleted = deleteTransactional(bdbTableAccessors.replicaPartitionAccessor.PKX, subPartition.getPartitionId());
        if (!deleted) {
            LOG.warn("this sub-partition has been already deleted?? :" + subPartition);
        }
    }

    @Override
    public LVColumnFile getColumnFile(int columnFileId) throws IOException {
        return getTransactional(bdbTableAccessors.columnFileAccessor.PKX, columnFileId);
    }

    @Override
    public LVColumnFile[] getAllColumnFilesByReplicaPartitionId(int subPartitionId) throws IOException {
        Collection<LVColumnFile> values = fetchAll(bdbTableAccessors.columnFileAccessor.IX_PARTITION_ID, subPartitionId);
        SortedMap<Integer, LVColumnFile> orderedMap = new TreeMap<Integer, LVColumnFile>(); // sorted by column order
        for (LVColumnFile file : values) {
            LVColumn column = getColumn(file.getColumnId());
            assert (column != null);
            assert (!orderedMap.containsKey(column.getOrder()));
            orderedMap.put(column.getOrder(), file);
        }
        return orderedMap.values().toArray(new LVColumnFile[orderedMap.size()]);
    }

    @Override
    public LVColumnFile getColumnFileByReplicaPartitionAndColumn(int subPartitionId, int columnId) throws IOException {
        // partition ID is enough selective. no composite index is needed.
        for (LVColumnFile file : fetchAll(bdbTableAccessors.columnFileAccessor.IX_PARTITION_ID, subPartitionId)) {
            if (file.getColumnId() == columnId) {
                return file;
            }
        }
        return null;
    }

    @Override
    public LVColumnFile createNewColumnFile(int subPartitionId, int columnId,
                    String localFilePath, int fileSize, int tupleCount,
                    long checksum, byte dictionaryBytesPerEntry, int distinctValues, int runCount, int uncompressedSizeKB) throws IOException {
        LVReplicaPartition subPartition = getReplicaPartition(subPartitionId);
        LVColumn column = getColumn(columnId);
        LVReplica replica = getReplica(subPartition.getReplicaId());
        LVReplicaScheme scheme = getReplicaScheme(replica.getSchemeId());
        final LVColumnFile file = new LVColumnFile();
        file.setColumnFileId(bdbTableAccessors.columnFileAccessor.issueNewId());
        file.setColumnId(column.getColumnId());
        file.setFileSize(fileSize);
        file.setLocalFilePath(localFilePath);
        file.setPartitionId(subPartition.getPartitionId());
        file.setChecksum(checksum);
        file.setDictionaryBytesPerEntry(dictionaryBytesPerEntry);
        file.setDistinctValues(distinctValues);
        file.setRunCount(runCount);
        file.setUncompressedSizeKB(uncompressedSizeKB);
        // de-normalization attributes
        file.setColumnType(column.getType());
        file.setCompressionType(scheme.getColumnCompressionScheme(column.getColumnId()));
        file.setSorted(scheme.getSortColumnId() != null && scheme.getSortColumnId().intValue() == column.getColumnId());
        file.setTupleCount(tupleCount);

        putNoReturnTransactional(bdbTableAccessors.columnFileAccessor.PKX, file);
        return file;
    }
    @Override
    public int createNewColumnFileIdOnlyReturn(int subPartitionId, int columnId, String localFilePath, int fileSize, int tupleCount, long checksum,
                    byte dictionaryBytesPerEntry, int distinctValues, int runCount, int uncompressedSizeKB) throws IOException {
        return createNewColumnFile(subPartitionId, columnId, localFilePath, fileSize, tupleCount, checksum, dictionaryBytesPerEntry, distinctValues, runCount, uncompressedSizeKB).getColumnFileId();
    }

    @Override
    public LVColumnFile updateColumnFilePath(int columnFileId, String newLocalFilePath) throws IOException {
        final LVColumnFile file = getColumnFile(columnFileId);
        file.setLocalFilePath(newLocalFilePath);
        putNoReturnTransactional(bdbTableAccessors.columnFileAccessor.PKX, file);
        return file;
    }
    @Override
    public void updateColumnFilePathNoReturn(int columnFileId, String newLocalFilePath) throws IOException {
        updateColumnFilePath (columnFileId, newLocalFilePath);
    }

    @Override
    public void dropColumnFile(final int columnFileId) throws IOException {
        assert (columnFileId > 0);
        boolean deleted = deleteTransactional(bdbTableAccessors.columnFileAccessor.PKX, columnFileId);
        if (!deleted) {
            LOG.warn("this column file has been already deleted?? :" + columnFileId);
        }
    }

    @Override
    public String queryColumnFilePlacement(String hdfsFilePath) throws IOException {
        LVFSFilePath path = new LVFSFilePath(hdfsFilePath);
        LVTable table = getTable(path.getTableId());
        if (table.isPervasiveReplication()) {
            return null; // null means replicating to all nodes
        }
        int replicaPartitionId = path.getReplicaPartitionId();
        LVReplicaPartition replicaPartition = getReplicaPartition(replicaPartitionId);
        Integer nodeId = replicaPartition.getNodeId();
        if (nodeId == null) {
            LOG.error("this column file belongs to a replica partition which hasn't been assigned to node?? :" + hdfsFilePath);
            throw new IOException ("this column file belongs to a replica partition which hasn't been assigned to node?? :" + hdfsFilePath);
        }
        return getRackNode(nodeId).getName();
    }

    @Override
    public LVRack getRack(int rackId) throws IOException {
        return getTransactional(bdbTableAccessors.rackAccessor.PKX, rackId);
    }

    @Override
    public LVRack getRack(String rackName) throws IOException {
        return getTransactional(bdbTableAccessors.rackAccessor.IX_NAME, rackName);
    }

    @Override
    public LVRack[] getAllRacks() throws IOException {
        // ID order
        return bdbTableAccessors.rackAccessor.PKX.sortedMap().values().toArray(new LVRack[0]);
    }

    @Override
    public LVRack createNewRack(String name) throws IOException {
        LOG.info("creating new rack " + name + "...");
        if (name == null || name.length() == 0) {
            throw new IOException ("empty rack name");
        }
        if (bdbTableAccessors.rackAccessor.IX_NAME.contains(name)) {
            throw new IOException ("this rack name already exists:" + name);
        }
        LVRack rack = new LVRack();
        rack.setRackId(bdbTableAccessors.rackAccessor.issueNewId());
        rack.setName(name);
        rack.setStatus(RackStatus.OK);
        putNoReturnTransactional(bdbTableAccessors.rackAccessor.PKX, rack);
        LOG.info("created new rack: " + rack);
        return rack;
    }

    @Override
    public LVRack updateRackStatus(LVRack rack, RackStatus status) throws IOException {
        assert (status != null);
        assert (rack.getRackId() > 0);
        rack.setStatus(status);
        putNoReturnTransactional(bdbTableAccessors.rackAccessor.PKX, rack);
        return rack;
    }

    @Override
    public void dropRack(LVRack rack) throws IOException {
        assert (rack.getRackId() > 0);
        LOG.info("dropping a rack: " + rack);
        for (LVRackNode node : getAllRackNodes(rack.getRackId())) {
            dropRackNode(node);
        }
        for (LVRackAssignment assignment : getAllRackAssignmentsByRackId(rack.getRackId())) {
            dropRackAssignment(assignment);
        }
        deleteTransactional(bdbTableAccessors.rackAccessor.PKX, rack.getRackId());
        LOG.info("dropped");
    }

    @Override
    public LVRackNode getRackNode(int nodeId) throws IOException {
        return getTransactional(bdbTableAccessors.rackNodeAccessor.PKX, nodeId);
    }

    @Override
    public LVRackNode getRackNode(String nodeName) throws IOException {
        return getTransactional(bdbTableAccessors.rackNodeAccessor.IX_NAME, nodeName);
    }

    @Override
    public LVRackNode[] getAllRackNodes(int rackId) throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.rackNodeAccessor.IX_RACK_ID, rackId).toArray(new LVRackNode[0]);
    }

    @Override
    public LVRackNode createNewRackNode(LVRack rack, String name, String address) throws IOException {
        assert (rack.getRackId() > 0);
        if (name == null || name.length() == 0) {
            throw new IOException ("empty node name");
        }
        if (address == null || address.length() == 0) {
            throw new IOException ("empty address");
        }
        if (bdbTableAccessors.rackNodeAccessor.IX_NAME.contains(name)) {
            throw new IOException ("this node name already exists:" + name);
        }
        LVRackNode node = new LVRackNode();
        node.setNodeId(bdbTableAccessors.rackNodeAccessor.issueNewId());
        node.setName(name);
        node.setAddress(address);
        node.setStatus(RackNodeStatus.OK);
        node.setRackId(rack.getRackId());
        putNoReturnTransactional(bdbTableAccessors.rackNodeAccessor.PKX, node);
        LOG.info("created new node: " + node);
        return node;
    }
    @Override
    public int createNewRackNodeIdOnlyReturn(int rackId, String name, String address) throws IOException {
        LVRack rack = getRack(rackId);
        return createNewRackNode(rack, name, address).getPrimaryKey();
    }

    @Override
    public LVRackNode updateRackNodeStatus(LVRackNode node, RackNodeStatus status) throws IOException {
        assert (node.getNodeId() > 0);
        assert (status != null);
        node.setStatus(status);
        putNoReturnTransactional(bdbTableAccessors.rackNodeAccessor.PKX, node);
        return node;
    }
    @Override
    public void updateRackNodeStatusNoReturn(int nodeId, RackNodeStatus status) throws IOException {
        LVRackNode node = getRackNode (nodeId);
        updateRackNodeStatus(node, status);
    }
    @Override
    public LVRackNode updateRackNodeAddress(LVRackNode node, String address) throws IOException {
        assert (node.getNodeId() > 0);
        assert (address != null && address.length() > 0);
        node.setAddress(address);
        putNoReturnTransactional(bdbTableAccessors.rackNodeAccessor.PKX, node);
        return node;
    }
    @Override
    public void updateRackNodeAddressNoReturn(int nodeId, String address) throws IOException {
        LVRackNode node = getRackNode (nodeId);
        updateRackNodeAddress(node, address);
    }

    @Override
    public void dropRackNode(LVRackNode node) throws IOException {
        assert (node.getNodeId() > 0);
        // nullify partition's assignments on this node
        for (LVReplicaPartition partition : fetchAll(bdbTableAccessors.replicaPartitionAccessor.IX_NODE_ID, node.getNodeId())) {
            partition.setNodeId(null);
            if (partition.getStatus() == ReplicaPartitionStatus.OK) {
                partition.setStatus(ReplicaPartitionStatus.LOST);
            }
            bdbTableAccessors.replicaPartitionAccessor.PKX.put(partition);
        }
        deleteTransactional(bdbTableAccessors.rackNodeAccessor.PKX, node.getNodeId());
    }
    
    @Override
    public int getReplicaPartitionCountInNode(LVRackNode node) throws IOException {
        return (int) fetchAll(bdbTableAccessors.replicaPartitionAccessor.IX_NODE_ID, node.getNodeId()).size();
    }

    @Override
    public LVRackAssignment getRackAssignment(int assignmentId) throws IOException {
        return getTransactional(bdbTableAccessors.rackAssignmentAccessor.PKX, assignmentId);
    }
    
    @Override
    public LVRackAssignment getRackAssignment(LVRack rack, LVFracture fracture) throws IOException {
        // #assignments per fracture and rack shouldn't be large.. so, joining two indexes is not so slower than composite index
        for (LVRackAssignment value : fetchAll(bdbTableAccessors.rackAssignmentAccessor.IX_FRACTURE_ID, fracture.getFractureId())) {
            if (value.getRackId() == rack.getRackId()) {
                return value;
            }
        }
        return null;
    }

    @Override
    public LVRackAssignment[] getAllRackAssignmentsByRackId(int rackId) throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.rackAssignmentAccessor.IX_RACK_ID, rackId).toArray(new LVRackAssignment[0]);
    }

    @Override
    public LVRackAssignment[] getAllRackAssignmentsByFractureId(int fractureId) throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.rackAssignmentAccessor.IX_FRACTURE_ID, fractureId).toArray(new LVRackAssignment[0]);
    }

    @Override
    public LVRackAssignment createNewRackAssignment(LVRack rack, LVFracture fracture, LVReplicaGroup owner) throws IOException {
        assert (rack.getRackId() > 0);
        assert (fracture.getFractureId() > 0);
        assert (owner.getGroupId() > 0);
        LVRackAssignment assignment = getRackAssignment(rack, fracture);
        if (assignment != null) {
            throw new IOException ("an assignment for this rack and fracture already exists:" + assignment);
        }
        assignment = new LVRackAssignment();
        assignment.setAssignmentId(bdbTableAccessors.rackAssignmentAccessor.issueNewId());
        assignment.setFractureId(fracture.getFractureId());
        assignment.setRackId(rack.getRackId());
        assignment.setOwnerReplicaGroupId(owner.getGroupId());
        putNoReturnTransactional(bdbTableAccessors.rackAssignmentAccessor.PKX, assignment);
        return assignment;
    }

    @Override
    public LVRackAssignment updateRackAssignmentOwner(LVRackAssignment assignment, LVReplicaGroup owner) throws IOException {
        assert (assignment.getAssignmentId() > 0);
        assert (owner.getGroupId() > 0);
        assignment.setOwnerReplicaGroupId(owner.getGroupId());
        putNoReturnTransactional(bdbTableAccessors.rackAssignmentAccessor.PKX, assignment);
        return assignment;
    }

    @Override
    public void dropRackAssignment(LVRackAssignment assignment) throws IOException {
        assert (assignment.getAssignmentId() > 0);
        deleteTransactional(bdbTableAccessors.rackAssignmentAccessor.PKX, assignment.getAssignmentId());
    }

    
    @Override
    public LVJob getJob(int jobId) throws IOException {
        return getTransactional(bdbTableAccessors.jobAccessor.PKX, jobId);
    }

    @Override
    public LVJob[] getAllJobs() throws IOException {
        // ID order
        return bdbTableAccessors.jobAccessor.PKX.sortedMap().values().toArray(new LVJob[0]);
    }

    @Override
    public LVJob createNewJob(String description, JobType type, byte[] parameters) throws IOException {
        LVJob job = new LVJob();
        job.setJobId(bdbTableAccessors.jobAccessor.issueNewId());
        job.setDescription(description == null ? "" : description);
        job.setProgress(0.0d);
        job.setStartedTime(new Date());
        job.setType(type);
        job.setStatus(JobStatus.CREATED);
        job.setParameters(parameters);
        putNoReturnTransactional(bdbTableAccessors.jobAccessor.PKX, job);
        return job;
    }

    @Override
    public int createNewJobIdOnlyReturn(String description, JobType type, byte[] parameters) throws IOException {
        return createNewJob(description, type, parameters).getJobId();
    }

    @Override
    public LVJob updateJob(int jobId, JobStatus status, DoubleWritable progress, String errorMessages) throws IOException {
        LVJob job = getJob(jobId);
        if (status != null) {
            boolean wasFinished = JobStatus.isFinished(job.getStatus());
            job.setStatus(status);
            if (!wasFinished && JobStatus.isFinished(status)) {
                job.setFinishedTime(new Date());
            }
        }
        if (progress != null) {
            job.setProgress(progress.get());
        }
        if (errorMessages != null) {
            job.setErrorMessages(errorMessages);
        }
        putNoReturnTransactional(bdbTableAccessors.jobAccessor.PKX, job);
        return job;
    }

    @Override
    public void updateJobNoReturn(int jobId, JobStatus status, DoubleWritable progress, String errorMessages) throws IOException {
        updateJob(jobId, status, progress, errorMessages);
    }

    @Override
    public void dropJob(int jobId) throws IOException {
        // delete sub tasks
        for (LVTask task : getAllTasksByJob(jobId)) {
            dropTask (task.getTaskId());
        }
        boolean deleted = deleteTransactional(bdbTableAccessors.jobAccessor.PKX, jobId);
        if (!deleted) {
            LOG.warn("Job-" + jobId + " doesn't exist (already deleted?)");
        }
    }

    
    @Override
    public LVTask getTask(int taskId) throws IOException {
        return getTransactional(bdbTableAccessors.taskAccessor.PKX, taskId);
    }

    @Override
    public LVTask[] getAllTasksByJob(int jobId) throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.taskAccessor.IX_JOB_ID, jobId).toArray(new LVTask[0]);
    }
    @Override
    public LVTask[] getAllTasksByNode(int nodeId) throws IOException {
        // ID order
        return fetchAll(bdbTableAccessors.taskAccessor.IX_NODE_ID, nodeId).toArray(new LVTask[0]);
    }
    
    @Override
    public LVTask[] getAllTasksByNodeAndStatus(int nodeId, TaskStatus status) throws IOException {
        // status (probably "REQUESTED") should be quite selective
        // because most of records will be historical (DONE/ERROR/etc status).
        // if this is not true, we should add a composite index on Status and NodeID.
        ArrayList<LVTask> tasks = new ArrayList<LVTask>();
        for (LVTask task : fetchAll(bdbTableAccessors.taskAccessor.IX_STATUS, status)) { // ID order
            if (task.getNodeId() == nodeId) {
                tasks.add(task);
            }
        }
        return tasks.toArray(new LVTask[tasks.size()]);
    }

    @Override
    public LVTask createNewTask(int jobId, int nodeId, TaskType type, byte[] parameters) throws IOException {
        LVTask task = new LVTask();
        task.setJobId(jobId);
        task.setNodeId(nodeId);
        task.setTaskId(bdbTableAccessors.taskAccessor.issueNewId());
        task.setProgress(0.0d);
        task.setStartedTime(new Date());
        task.setType(type);
        task.setStatus(TaskStatus.CREATED);
        task.setParameters(parameters);
        putNoReturnTransactional(bdbTableAccessors.taskAccessor.PKX, task);
        return task;
    }

    @Override
    public int createNewTaskIdOnlyReturn(int jobId, int nodeId, TaskType type, byte[] parameters) throws IOException {
        return createNewTask(jobId, nodeId, type, parameters).getTaskId();
    }

    @Override
    public LVTask updateTask(int taskId, TaskStatus status, DoubleWritable progress, String[] outputFilePaths, String errorMessages) throws IOException {
        LVTask task = getTask(taskId);
        if (status != null) {
            boolean wasFinished = TaskStatus.isFinished(task.getStatus());
            boolean wasStarted = status == TaskStatus.RUNNING && task.getStatus() == TaskStatus.START_REQUESTED;
            task.setStatus(status);
            if (!wasFinished && TaskStatus.isFinished(status)) {
                task.setFinishedTime(new Date());
            }
            if (wasStarted) {
                task.setStartedTime(new Date());
            }
        }
        if (progress != null) {
            task.setProgress(progress.get());
        }
        if (outputFilePaths != null) {
            task.setOutputFilePaths(outputFilePaths);
        }
        if (errorMessages != null) {
            task.setErrorMessages(errorMessages);
        }
        putNoReturnTransactional(bdbTableAccessors.taskAccessor.PKX, task);
        return task;
    }

    @Override
    public void updateTaskNoReturn(int taskId, TaskStatus status, DoubleWritable progress, String[] outputFilePaths, String errorMessages) throws IOException {
        updateTask(taskId, status, progress, outputFilePaths, errorMessages);
    }

    @Override
    public void dropTask(int taskId) throws IOException {
        boolean deleted = deleteTransactional(bdbTableAccessors.taskAccessor.PKX, taskId);
        if (!deleted) {
            LOG.warn("Task-" + taskId + " doesn't exist (already deleted?)");
        }
    }

    /**
     * helper class to do something in a transaction.
     * the metadata repository is not 100% ACID. rather, be careful to not
     * cause any deadlocks. doTxn() should contain minimal stuffs in it.
     */
    private abstract class TransactionalSection {
        protected abstract void doTxn(Transaction txn) throws IOException;
        protected Object ret;
        /** use this to return optional return values. */
        protected final void setRet (Object ret) {
            this.ret = ret;
        }
        private static final int RETRY_MAX = 20;
        public final Object run () throws IOException {
            LockTimeoutException lastEx = null;
            for (int retry = 0; retry < RETRY_MAX; ++retry) {
                try {
                    TransactionConfig txnConfig = new TransactionConfig();
                    txnConfig.setReadCommitted(true);
                    txnConfig.setReadUncommitted(false);
                    txnConfig.setSerializableIsolation(false);
                    Transaction txn = bdbEnv.beginTransaction(null, txnConfig);
                    boolean committed = false;
                    try {
                        doTxn(txn);
                        txn.commit();
                        committed = true;
                    } finally {
                        if (!committed) {
                            txn.abort();
                        }
                    }
                    return ret;
                } catch (LockTimeoutException ex) {
                    int randomWait = new Random(System.nanoTime()).nextInt(300);
                    LOG.warn("lock timeout happened. retrying(" + retry + ") with wait " + randomWait + "ms");
                    lastEx = ex;
                    // to avoid further lock conflict, sleep for random interval
                    try {
                        Thread.sleep(randomWait);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            LOG.error("lock timeout happened too many times. gave up", lastEx);
            throw lastEx;
        }
    }

    /**
     * Inserts/updates an object in an independent transaction (not for full ACID. only record-level consistency, instead this won't cause deadlocks).
     * these function also internally do retry to avoid lock timeouts.
     */
    private <T> void putNoReturnTransactional (final PrimaryIndex<Integer, T> pkx, final T obj) throws IOException {
        new TransactionalSection() {
            public void doTxn(Transaction txn) throws IOException {
                pkx.putNoReturn(txn, obj);
            }
        }.run();
    }

    /** Deletes an object in an independent transaction (not for full ACID. only record-level consistency, instead this won't cause deadlocks). */
    private <T> boolean deleteTransactional (final PrimaryIndex<Integer, T> pkx, final int objId) throws IOException {
        return (Boolean) new TransactionalSection() {
            public void doTxn(Transaction txn) throws IOException {
                boolean deleted = pkx.delete(txn, objId);
                setRet(deleted);
            }
        }.run();
    }

    /** Get version (primary index), same as above. */
    @SuppressWarnings("unchecked")
    private <T> T getTransactional (final PrimaryIndex<Integer, T> pkx, final int objId) throws IOException {
        return (T) new TransactionalSection() {
            public void doTxn(Transaction txn) throws IOException {
                setRet(pkx.get(txn, objId, LockMode.READ_COMMITTED));
            }
        }.run();
    }
    /** Get version (unique secondary index), same as above. */
    @SuppressWarnings("unchecked")
    private <K, T> T getTransactional (final SecondaryIndex<K, Integer, T> index, final K key) throws IOException {
        return (T) new TransactionalSection() {
            public void doTxn(Transaction txn) throws IOException {
                setRet(index.get(txn, key, LockMode.READ_COMMITTED));
            }
        }.run();
    }

    /**
     * Retrieves all (duplicate) entries from the secondary index for the given key.
     * This method releases locks after reading each record and returns a separated (as opposed
     * to BDB's own cursor) list. Again, deadlock avoidance at the cost of ACID.
     * The result is sorted by ID.
     */
    @SuppressWarnings("unchecked")
    private <K extends Comparable<K>, T extends LVObject> Collection<T> fetchAll (final SecondaryIndex<K, Integer, T> index, final K key) throws IOException {
        return (Collection<T>) new TransactionalSection() {
            @Override
            protected void doTxn(Transaction txn) throws IOException {
                SortedMap<Integer, T> map = new TreeMap<Integer, T>();
                EntityIndex<Integer, T> subIndex = index.subIndex(key);
                CursorConfig config = new CursorConfig();
                config.setReadCommitted(true);
                config.setReadUncommitted(false);
                EntityCursor<T> cursor = subIndex.entities(txn, config);
                try {
                    while (true) {
                        T obj = cursor.next();
                        if (obj == null) {
                            break;
                        }
                        map.put (obj.getPrimaryKey(), obj);
                    }
                } finally {
                    cursor.close();
                }
                setRet(map.values());
            }
        }.run();
    }

    /**
     * Primary index version, same as above.
     */
    @SuppressWarnings("unchecked")
    private <T extends LVObject> Collection<T> fetchAll (final PrimaryIndex<Integer, T> pkx) throws IOException {
        return (Collection<T>) new TransactionalSection() {
            @Override
            protected void doTxn(Transaction txn) throws IOException {
                SortedMap<Integer, T> map = new TreeMap<Integer, T>();
                CursorConfig config = new CursorConfig();
                config.setReadCommitted(true);
                config.setReadUncommitted(false);
                EntityCursor<T> cursor = pkx.entities(txn, config);
                try {
                    while (true) {
                        T obj = cursor.next();
                        if (obj == null) {
                            break;
                        }
                        map.put (obj.getPrimaryKey(), obj);
                    }
                } finally {
                    cursor.close();
                }
                setRet(map.values());
            }
        }.run();
    }
}
