package edu.brown.lasvegas.lvfs.meta;

import java.io.IOException;
import java.util.Map;

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

/**
 * Represents a repository of all metadata in LVFS.
 * 
 * <p>All write accesses to this repository are durably stored in our name-node.
 * The implementation of this interface might be the actual name-node, or a read-only slave in
 * other nodes which just forwards write-accesses to the name-node.</p>
 * 
 * <p>Note that this repository only maintains metadata.
 * Adding or deleting objects in this repository does not mean the corresponding
 * HDFS files are actually created.</p>
 * 
 * <p>A side note. getXxx usually receives integer IDs, not the corresponding objects.
 * For example, getAllFractures(int tableId), not getAllFractures(LVTable table).
 * This is because these getters might be frequently called without the "parent"
 * object. OTOH, createXxx always receives object because they will be much less
 * frequently called and materializing the parent object is not an issue,
 * so type safety wins.</p>
 */
public interface MetadataRepository {
    /**
     * Epoch is a coarse grained timestamp to partition inserted tuples. It's maintained
     * as a hidden implicit column in each table. Usually, one epoch corresponds to millions of tuples.
     * @return sequentially increasing epoch number.
     */
    int issueNewEpoch() throws IOException;
    
    /**
     * Issues a unique ID for each metadata object. 
     * @param clazz specified the metadata object.
     * @return unique ID for the class.
     */
    int issueNewId(Class<?> clazz) throws IOException;
    
    /**
     * Assures every change in this repository is flushed to disk.
     * @throws IOException
     */
    void sync() throws IOException;

    //////////////////////// Table Methods : begin ////////////////////////////////
    /**
     * Not only making everything durable, this method compacts
     * internal data in storage layer to speed up next start-up. 
     * (sort of obscure description to abstract the internal.
     * in short, call this function occasionally. It's not mandatory.)
     * @throws IOException
     */
    void checkpoint() throws IOException;
    
    /**
     * Returns the table object with the given ID.
     * @param tableId Table ID 
     * @return table object. null if the ID is not found.
     * @throws IOException
     */
    LVTable getTable(int tableId) throws IOException;
    
    /**
     * Creates a new table with the given columns and base partitioning scheme.
     * This method itself does not create any replica scheme for this table.
     * You have to create at least one replica scheme to this table before importing fractures.
     * @param name the name of the new table
     * @param columns spec of the columns in the table (ID/Order is ignored)
     * The epoch column is automatically added as an implicit column. 
     * @param basePartitioningColumnOrder the partitioning column of the base replica group for this table.
     * Specified by the column order (0=epoch, 1=columns[0], 2=columns[1], ...).
     * @return the newly created table
     * @throws IOException
     * @see {@link #createNewReplicaScheme(LVReplicaGroup, LVTableColumn, Map)}
     */
    LVTable createNewTable (String name, LVTableColumn[] columns, int basePartitioningColumnOrder) throws IOException;
    
    /**
     * Drops a table and all of its column files in all replicas.
     * Dropping a table consists of a few steps which might take long time.
     * This method merely sets a flag so that the steps will start in near future.
     * @param table the table to drop
     * @throws IOException
     */
    void requestDropTable (LVTable table) throws IOException;
    
    /**
     * This method called to fully delete the metadata object of the table and
     * related objects from this
     * repository after the deletion actually completes.
     * @param table the table to drop
     * @throws IOException
     */
    void dropTable (LVTable table) throws IOException;

    //////////////////////// Column Methods : begin ////////////////////////////////
    /**
     * Returns all columns in the specified table. 
     * @param tableId Table ID
     * @return column objects, in the order of {@link LVTableColumn#getOrder()}.
     * @throws IOException
     */
    LVTableColumn[] getAllColumns(int tableId) throws IOException;

    /**
     * Returns the column object with the given ID. 
     * @param columnId Column ID
     * @return column object. null if the ID is not found.
     * @throws IOException
     */
    LVTableColumn getColumn(int columnId) throws IOException;

    /**
     * Add a new column to the last of an existing table.
     * The new column has NULL as the value of all tuples.
     * Only subsequently added tuples can have non-NULL values for the column.
     * @param table the existing table to add the column
     * @param name the name of the new column
     * @param type the data type of the new column
     * @return the newly added column
     * @throws IOException
     */
    LVTableColumn createNewColumn (LVTable table, String name, ColumnType type) throws IOException;
    
    /**
     * Drops a column and all of its column files in all replicas.
     * Dropping a column consists of a few steps which might take long time.
     * This method merely sets a flag so that the steps will start in near future.
     * @param column the column to drop
     * @throws IOException
     */
    void requestDropColumn (LVTableColumn column) throws IOException;

    /**
     * This method called to fully delete the metadata object of the column and
     * related objects from this
     * repository after the deletion actually completes.
     * @param column the column to drop
     * @throws IOException
     */
    void dropColumn (LVTableColumn column) throws IOException;
    
    //////////////////////// Fracture Methods : begin ////////////////////////////////
    /**
     * Returns the fracture object with the given ID. 
     * @param fractureId Fracture ID
     * @return fracture object. null if the ID is not found.
     * @throws IOException
     */
    LVTableFracture getFracture(int fractureId) throws IOException;

    /**
     * Returns all fractures in the table. 
     * @param tableId Table that contains the fractures.
     * @return fracture objects. not in a particular order.
     * @throws IOException
     */
    LVTableFracture[] getAllFractures(int tableId) throws IOException;

    /**
     * Creates a new fracture in the given table. As of this method call,
     * the caller doesn't have to know the exact key range or tuple count.
     * So, this method merely acquires a unique ID for the new fracture.
     * As soon as the key range and tuple counts are finalized, call
     * {@link #finalizeFracture(LVTableFracture)}.
     * @param table the table to create a new fracture
     * @return new Fracture object
     * @throws IOException
     */
    LVTableFracture createNewFracture(LVTable table) throws IOException;

    /**
     * Saves and finalizes the fracture definition.
     * A newly created fracture is inactive until this method call.
     * @param fracture the fracture object with full details (eg key range).
     * @throws IOException
     */
    void finalizeFracture(LVTableFracture fracture) throws IOException;
    
    /**
     * Deletes the fracture metadata object and related objects from this repository.
     * This is called only when some fractures are merged.
     * @param fracture the fracture object to delete
     * @throws IOException
     */
    void dropFracture (LVTableFracture fracture) throws IOException;

    //////////////////////// Replica Group Methods : begin ////////////////////////////////
    /**
     * Returns the replica group object with the given ID. 
     * @param groupId Group ID
     * @return replica group object. null if the ID is not found.
     * @throws IOException
     */
    LVReplicaGroup getReplicaGroup(int groupId) throws IOException;

    /**
     * Returns all replica groups in the table. 
     * @param tableId Table ID.
     * @return replica group objects. not in a particular order.
     * @throws IOException
     */
    LVReplicaGroup[] getAllReplicaGroups(int tableId) throws IOException;
    /**
     * Returns the base replica group of the table. 
     * @param tableId Table ID.
     * @return the base group of the table
     * @throws IOException
     */
    LVReplicaGroup getBaseReplicaGroup(int tableId) throws IOException;

    /**
     * Creates a new additional replica group in the given table with the specified partitioning column.
     * This method can't add a base group, which has to be created when the table is created.
     * @param table the table to create a new group
     * @param partitioningColumn the partitioning column of the new group
     * @return new ReplicaGroup object
     * @throws IOException
     */
    LVReplicaGroup createNewReplicaGroup(LVTable table, LVTableColumn partitioningColumn) throws IOException;
    
    /**
     * Deletes the replica group metadata object and related objects from this repository.
     * The base replica group must not be deleted unless this is part of dropTable.
     * @param group the replica group object to delete
     * @throws IOException
     */
    void dropReplicaGroup (LVReplicaGroup group) throws IOException;

    //////////////////////// Replica Scheme Methods : begin ////////////////////////////////
    /**
     * Returns the replica scheme object with the given ID. 
     * @param schemeId Replica Scheme ID
     * @return replica scheme object. null if the ID is not found.
     * @throws IOException
     */
    LVReplicaScheme getReplicaScheme(int schemeId) throws IOException;

    /**
     * Returns all replica schemes in the specified replica group. 
     * @param groupId Replica Scheme ID.
     * @return replica scheme objects. not in a particular order.
     * @throws IOException
     */
    LVReplicaScheme[] getAllReplicaSchemes(int groupId) throws IOException;

    /**
     * Creates a new additional replica group in the given table
     * with the specified in-block sorting column and compression scheme.
     * Column compression scheme can be changed later.
     * Call {@link #changeColumnCompressionScheme(LVReplicaScheme, LVTableColumn, CompressionType)}
     * afterwards to do so.
     * @param group the replica group to create a replica scheme
     * @param sortingColumn the in-block sorting column of the replica scheme
     * @param columnCompressionSchemes compression schemes of each column (key=Column Id)
     * omitted columns are considered as no-compression.
     * @return new ReplicaScheme object
     * @throws IOException
     */
    LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVTableColumn sortingColumn,
                    Map<Integer, CompressionType> columnCompressionSchemes) throws IOException;


    /**
     * Changes a compression scheme of the specified column.
     * Applying the new compression scheme will happen asynchronously.
     * @param scheme the replica scheme to apply the new compression scheme
     * @param column the column to change compression scheme
     * @param compressionType new compression scheme
     * @return modified replica scheme object
     * @throws IOException
     */
    LVReplicaScheme changeColumnCompressionScheme(LVReplicaScheme scheme,
                    LVTableColumn column, CompressionType compressionType) throws IOException;
    
    /**
     * Deletes the replica scheme metadata object and related objects from this repository.
     * @param scheme the replica scheme object to delete
     * @throws IOException
     */
    void dropReplicaScheme (LVReplicaScheme scheme) throws IOException;

    //////////////////////// Replica (Replicated Fracture) Methods : begin ////////////////////////////////
    /**
     * Returns the replica object with the given ID. 
     * @param replicaId Replica ID
     * @return replica object. null if the ID is not found.
     * @throws IOException
     */
    LVReplica getReplica(int replicaId) throws IOException;

    /**
     * Returns all replicas in the given replica scheme.
     * @param schemeId Replica scheme the replicas are based on.
     * @return replica objects. not in a particular order.
     * @throws IOException
     */
    LVReplica[] getAllReplicasBySchemeId(int schemeId) throws IOException;

    /**
     * Returns all replicas of the given fracture.
     * @param fractureId Fracture the replicas are based on.
     * @return replica objects. not in a particular order.
     * @throws IOException
     */
    LVReplica[] getAllReplicasByFractureId(int fractureId) throws IOException;

    /**
     * Returns the replica object of the given replica scheme and the fracture. 
     * @param schemeId Replica scheme the replica is based on.
     * @param fractureId Fracture the replica is based on.
     * @return replica object. null if the ID is not found.
     * @throws IOException
     */
    LVReplica getReplicaFromSchemeAndFracture(int schemeId, int fractureId) throws IOException;

    /**
     * Creates a new replica for the given replica scheme and fracture.
     * @param scheme the replica scheme the new replica will be based on
     * @param fracture the fracture the new replica will be based on
     * @return new Replica object
     * @throws IOException
     */
    LVReplica createNewReplica(LVReplicaScheme scheme, LVTableFracture fracture) throws IOException;

    /**
     * Updates the status of the replica.
     * @param replica the replica object
     * @param status new status
     * @return modified Replica object
     * @throws IOException
     */
    LVReplica updateReplicaStatus(LVReplica replica, ReplicaStatus status) throws IOException;
    
    /**
     * Deletes the replica metadata object and related objects from this repository.
     * @param replica the replica object to delete
     * @throws IOException
     */
    void dropReplica (LVReplica replica) throws IOException;

    //////////////////////// Replica Partition Scheme (Sub-Partition Scheme) Methods : begin ////////////////////////////////
    /**
     * Returns the sub-partition scheme object with the given ID. 
     * @param subPartitionSchemeId the ID of sub-partition scheme
     * @return sub-partition scheme object. null if the ID is not found.
     * @throws IOException
     */
    LVReplicaPartitionScheme getReplicaPartitionScheme(int subPartitionSchemeId) throws IOException;

    /**
     * Returns all sub-partition scheme objects  in the given fracture.
     * @param fractureId Fracture the replica partition schemes are for.
     * @return sub-partition scheme objects. not in a particular order.
     * @throws IOException
     */
    LVReplicaPartitionScheme[] getAllReplicaPartitionSchemesByFractureId(int fractureId) throws IOException;

    /**
     * Returns all sub-partition scheme objects  of the given replica group.
     * @param groupId Replica Group the partition schemes are for.
     * @return sub-partition scheme objects. not in a particular order.
     * @throws IOException
     */
    LVReplicaPartitionScheme[] getAllReplicaPartitionSchemesByGroupId(int groupId) throws IOException;

    /**
     * Returns the sub-partition scheme object for the given . 
     * @param fractureId Fracture the sub-partition scheme is for.
     * @param groupId Replica Group the sub-partition scheme is for.
     * @return sub-partition scheme. null if the ID is not found.
     * @throws IOException
     */
    LVReplicaPartitionScheme getReplicaPartitionSchemeByFractureAndGroup(int fractureId, int groupId) throws IOException;

    /**
     * Creates a new sub-partition scheme for the given fracture and replica group.
     * As of this method call, the caller doesn't have to know the exact key range.
     * So, this method merely acquires a unique ID for the new sub-partition scheme.
     * As soon as the key ranges are finalized, call
     * {@link #finalizeReplicaPartitionScheme(LVReplicaPartitionScheme)}.
     * @param fracture Fracture the replica partition scheme is for.
     * @param group Replica Group the partition scheme is for.
     * @return new sub-partition scheme
     * @throws IOException
     */
    LVReplicaPartitionScheme createNewReplicaPartitionScheme(
                    LVTableFracture fracture, LVReplicaGroup group) throws IOException;

    /**
     * Saves and finalizes the sub-partition scheme definition.
     * A newly created sub-partition scheme is inactive until this method call.
     * @param subPartitionScheme the sub-partition scheme object with full details (eg key ranges).
     * @throws IOException
     */
    void finalizeReplicaPartitionScheme(LVReplicaPartitionScheme subPartitionScheme) throws IOException;
    
    /**
     * Deletes the sub-partition scheme metadata object and related objects from this repository.
     * @param subPartitionScheme the sub-partition scheme object to delete
     * @throws IOException
     */
    void dropReplicaPartitionScheme (LVReplicaPartitionScheme subPartitionScheme) throws IOException;

    //////////////////////// Replica Partition (Sub-Partition) Methods : begin ////////////////////////////////
    /**
     * Returns the sub-partition object with the given ID. 
     * @param subPartitionId the ID of sub-partition
     * @return sub-partition object. null if the ID is not found.
     * @throws IOException
     */
    LVReplicaPartition getReplicaPartition(int subPartitionId) throws IOException;

    /**
     * Returns all sub-partition objects  in the given replica (Replicated-Fracture).
     * @param replicaId Replica (Replicated-Fracture) the sub-partitions belong to.
     * @return sub-partition objects. in a range order.
     * @throws IOException
     */
    LVReplicaPartition[] getAllReplicaPartitionsByReplicaId(int replicaId) throws IOException;

    /**
     * Returns the sub-partition object of the given replica for the given range (index in sub-partition scheme's range definitions).
     * @param replicaId Replica (Replicated-Fracture) the sub-partition belongs to.
     * @param range key range the sub-partitions stores. index of {@link LVReplicaPartitionScheme#getRanges()}
     * @return sub-partition object.
     * @throws IOException
     */
    LVReplicaPartition getReplicaPartitionByReplicaAndRange(int replicaId, int range) throws IOException;

    /**
     * Creates a new sub-partition for the replica and range.
     * @param replica Replica (Replicated-Fracture) the sub-partition belongs to.
     * @param range key range the sub-partitions stores. index of {@link LVReplicaPartitionScheme#getRanges()}
     * @return new sub-partition
     * @throws IOException
     */
    LVReplicaPartition createNewReplicaPartition(LVReplica replica, int range) throws IOException;

    /**
     * Updates the sub-partition object. Called when the sub-partition is materialized,
     * corrupted, recovered, migrated etc.
     * @param subPartition the sub-partition object to update
     * @param status new status of the sub-partition
     * @param currentHdfsNodeUri new value for currentHdfsNodeUri 
     * @param recoveryHdfsNodeUri new value for recoveryHdfsNodeUri
     * @return modified sub-partition
     * @throws IOException
     */
    LVReplicaPartition updateReplicaPartition(LVReplicaPartition subPartition,
        ReplicaPartitionStatus status,
        String currentHdfsNodeUri,
        String recoveryHdfsNodeUri
        ) throws IOException;
    
    /**
     * Deletes the sub-partition metadata object and related objects from this repository.
     * @param subPartition the sub-partition object to delete
     * @throws IOException
     */
    void dropReplicaPartition (LVReplicaPartition subPartition) throws IOException;

    //////////////////////// Column File Methods : begin ////////////////////////////////
    /**
     * Returns the column file object with the given ID. 
     * @param columnFileId the ID of column file
     * @return column file object. null if the ID is not found.
     * @throws IOException
     */
    LVColumnFile getColumnFile(int columnFileId) throws IOException;

    /**
     * Returns all column file objects  in the given sub-partition.
     * @param subPartitionId the sub-partition the column files belong to.
     * @return column file objects. in a column order.
     * @throws IOException
     */
    LVColumnFile[] getAllColumnFilesByReplicaPartitionId(int subPartitionId) throws IOException;

    /**
     * Returns the column file object of the given sub-partition and the column.
     * @param subPartitionId the sub-partition the column file belongs to.
     * @param columnId Column-ID
     * @return column file object.
     * @throws IOException
     */
    LVColumnFile getColumnFileByReplicaPartitionAndColumn(int subPartitionId, int columnId) throws IOException;

    /**
     * Creates a new column file for the sub-partition and column.
     * Column file is purely write-once read-only, so there is no updateColumnFile() method.
     * @param subPartition the sub-partition the column file belongs to.
     * @param column Column
     * @param hdfsFilePath the file path of the column file in HDFS
     * @param fileSize the byte size of the file
     * @return new column file
     * @throws IOException
     */
    LVColumnFile createNewColumnFile(LVReplicaPartition subPartition, LVTableColumn column,
                    String hdfsFilePath, long fileSize) throws IOException;
    
    /**
     * Deletes the column file metadata object from this repository.
     * @param columnFile the column file object to delete
     * @throws IOException
     */
    void dropColumnFile (LVColumnFile columnFile) throws IOException;
}
