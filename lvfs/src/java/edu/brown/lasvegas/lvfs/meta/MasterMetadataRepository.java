package edu.brown.lasvegas.lvfs.meta;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

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
import edu.brown.lasvegas.TableStatus;

/**
 * Implementation of {@link MetadataRepository} in the master namenode.
 * This can directly handle all read and write accesses over the local BDB-JE
 * instance.
 */
public class MasterMetadataRepository implements MetadataRepository {
    private static Logger LOG = Logger.getLogger(MasterMetadataRepository.class);
    
    private final File bdbEnvHome;
    private final EnvironmentConfig bdbEnvConf;    
    private Environment bdbEnv;
    
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
        if (LOG.isInfoEnabled()) {
            LOG.info("creating new table " + name + "...");
        }
        assert (name.length() > 0);
        assert (basePartitioningColumnOrder >= 0);
        assert (basePartitioningColumnOrder <= columns.length);
        assert (columns.length > 0);
        if (bdbTableAccessors.tableAccessor.IX_NAME.contains(name)) {
            throw new IOException ("this table name already exists:" + name);
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
            column.setName("__epoch");
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVTableColumn getColumn(int columnId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVTableColumn createNewColumn(LVTable table, String name, ColumnType type) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void requestDropColumn(LVTableColumn column) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void dropColumn(LVTableColumn column) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVTableFracture getFracture(int fractureId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVTableFracture[] getAllFractures(int tableId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVTableFracture createNewFracture(LVTable table) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void finalizeFracture(LVTableFracture fracture) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void dropFracture(LVTableFracture fracture) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVReplicaGroup getReplicaGroup(int groupId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaGroup[] getAllReplicaGroups(int tableId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaGroup getBaseReplicaGroup(int tableId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaGroup createNewReplicaGroup(LVTable table, LVTableColumn partitioningColumn) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void dropReplicaGroup(LVReplicaGroup group) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVReplicaScheme getReplicaScheme(int schemeId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaScheme[] getAllReplicaSchemes(int groupId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVTableColumn sortingColumn, Map<Integer, CompressionType> columnCompressionSchemes)
                    throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaScheme changeColumnCompressionScheme(LVReplicaScheme scheme, LVTableColumn column, CompressionType compressionType) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void dropReplicaScheme(LVReplicaScheme scheme) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVReplica getReplica(int replicaId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplica[] getAllReplicasBySchemeId(int schemeId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplica[] getAllReplicasByFractureId(int fractureId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplica getReplicaFromSchemeAndFracture(int schemeId, int fractureId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplica createNewReplica(LVReplicaScheme scheme, LVTableFracture fracture) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void finalizeReplica(LVReplica replica) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void dropReplica(LVReplica replica) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVReplicaPartitionScheme getReplicaPartitionScheme(int subPartitionSchemeId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartitionScheme[] getAllReplicaPartitionSchemesByFractureId(int fractureId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartitionScheme[] getAllReplicaPartitionSchemesByGroupId(int groupId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartitionScheme getReplicaPartitionSchemeByFractureAndGroup(int fractureId, int groupId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartitionScheme createNewReplicaPartitionScheme(LVTableFracture fracture, LVReplicaGroup group) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void finalizeReplicaPartitionScheme(LVReplicaPartitionScheme subPartitionScheme) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void dropReplicaPartitionScheme(LVReplicaPartitionScheme subPartitionScheme) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVReplicaPartition getReplicaPartition(int subPartitionId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartition[] getAllReplicaPartitionsByReplicaId(int replicaId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartition getReplicaPartitionByReplicaAndRange(int replicaId, int range) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaPartition createNewReplicaPartition(LVReplica replica, int range) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void updateReplicaPartition(LVReplicaPartition subPartition, ReplicaPartitionStatus status, String currentHdfsNodeUri, String recoveryHdfsNodeUri)
                    throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void dropReplicaPartition(LVReplicaPartition subPartition) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVColumnFile getColumnFile(int columnFileId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVColumnFile[] getAllColumnFilesByReplicaPartitionId(int subPartitionId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVColumnFile getColumnFileByReplicaPartitionAndColumn(int subPartitionId, int columnId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVColumnFile createNewColumnFile(LVReplicaPartition subPartition, LVTableColumn column, String hdfsFilePath, long fileSize) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void dropColumnFile(LVColumnFile columnFile) throws IOException {
        // TODO Auto-generated method stub
        
    }
}
