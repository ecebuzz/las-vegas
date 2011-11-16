package edu.brown.lasvegas.lvfs.meta;

import java.io.IOException;
import java.util.Map;

import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVColumnType;
import edu.brown.lasvegas.LVCompressionType;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaPartitionScheme;
import edu.brown.lasvegas.LVReplicaPartitionStatus;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVTableColumn;
import edu.brown.lasvegas.LVTableFracture;

/**
 * Implementation of {@link MetadataRepository} in the master namenode.
 * This can directly handle all read and write accesses over the local BDB-JE
 * instance.
 */
public class MasterMetadataRepository implements MetadataRepository {

    @Override
    public int issueNewEpoch() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int issueNewId(Class<?> clazz) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void sync() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void checkpoint() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LVTable getTable(int tableId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVTable createNewTable(String name, LVTableColumn[] columns, LVReplicaGroup baseGroup) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void requestDropTable(LVTable table) throws IOException {
        // TODO Auto-generated method stub
        
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
    public LVTableColumn createNewColumn(LVTable table, String name, LVColumnType type) throws IOException {
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
    public LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVTableColumn sortingColumn, Map<Integer, LVCompressionType> columnCompressionSchemes)
                    throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LVReplicaScheme changeColumnCompressionScheme(LVReplicaScheme scheme, LVTableColumn column, LVCompressionType compressionType) throws IOException {
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
    public void updateReplicaPartition(LVReplicaPartition subPartition, LVReplicaPartitionStatus status, String currentHdfsNodeUri, String recoveryHdfsNodeUri)
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
