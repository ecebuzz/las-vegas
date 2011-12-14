package edu.brown.lasvegas.lvfs.meta;

import java.io.IOException;
import java.util.Map;

import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackAssignment;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaGroup;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVSubPartitionScheme;
import edu.brown.lasvegas.RackNodeStatus;
import edu.brown.lasvegas.RackStatus;
import edu.brown.lasvegas.ReplicaPartitionStatus;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVFracture;
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
     * Issues (reserves) a series of unique IDs at once for better performance.
     * @param clazz specified the metadata object.
     * @param blockSize number of IDs to reserve
     * @return the beginning of unique IDs for the class.
     */
    int issueNewIdBlock(Class<?> clazz, int blockSize) throws IOException;
    
    /**
     * Assures every change in this repository is flushed to disk.
     * @throws IOException
     */
    void sync() throws IOException;

    /**
     * Not only making everything durable, this method compacts
     * internal data in storage layer to speed up next start-up. 
     * (sort of obscure description to abstract the internal.
     * in short, call this function occasionally. It's not mandatory.)
     * @throws IOException
     */
    void checkpoint() throws IOException;

    /**
     * Releases every resource this repository holds.
     * @throws IOException
     */
    void close () throws IOException;

    //////////////////////// Table Methods : begin ////////////////////////////////
    
    /**
     * Returns the table object with the given ID.
     * @param tableId Table ID 
     * @return table object. null if the ID is not found.
     * @throws IOException
     */
    LVTable getTable(int tableId) throws IOException;
    
    /**
     * Returns the table object with the given name (case insensitive). 
     * @param name Table name 
     * @return table object. null if the name is not found.
     * @throws IOException
     */
    LVTable getTable(String name) throws IOException;

    /**
     * Creates a new table with the given columns and base partitioning scheme.
     * This method itself does not create any replica scheme for this table.
     * You have to create at least one replica scheme to this table before importing fractures.
     * @param name the name of the new table
     * @param columns spec of the columns in the table (ID/Order is ignored)
     * The epoch column is automatically added as an implicit column. 
     * @return the newly created table
     * @throws IOException
     * @see #createNewReplicaScheme(LVReplicaGroup, LVColumn, Map)
     */
    LVTable createNewTable (String name, LVColumn[] columns) throws IOException;
    
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
     * @return column objects, in the order of {@link LVColumn#getOrder()}.
     * @throws IOException
     */
    LVColumn[] getAllColumns(int tableId) throws IOException;

    /**
     * Returns the column object with the given ID. 
     * @param columnId Column ID
     * @return column object. null if the ID is not found.
     * @throws IOException
     */
    LVColumn getColumn(int columnId) throws IOException;

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
    LVColumn createNewColumn (LVTable table, String name, ColumnType type) throws IOException;
    
    /**
     * Drops a column and all of its column files in all replicas.
     * Dropping a column consists of a few steps which might take long time.
     * This method merely sets a flag so that the steps will start in near future.
     * @param column the column to drop
     * @throws IOException
     */
    void requestDropColumn (LVColumn column) throws IOException;

    /**
     * This method called to fully delete the metadata object of the column and
     * related objects from this
     * repository after the deletion actually completes.
     * @param column the column to drop
     * @throws IOException
     */
    void dropColumn (LVColumn column) throws IOException;
    
    //////////////////////// Fracture Methods : begin ////////////////////////////////
    /**
     * Returns the fracture object with the given ID. 
     * @param fractureId Fracture ID
     * @return fracture object. null if the ID is not found.
     * @throws IOException
     */
    LVFracture getFracture(int fractureId) throws IOException;

    /**
     * Returns all fractures in the table. 
     * @param tableId Table that contains the fractures.
     * @return fracture objects. in ID order.
     * @throws IOException
     */
    LVFracture[] getAllFractures(int tableId) throws IOException;

    /**
     * Creates a new fracture in the given table. As of this method call,
     * the caller doesn't have to know the exact key range or tuple count.
     * So, this method merely acquires a unique ID for the new fracture.
     * As soon as the key range and tuple counts are finalized, call
     * {@link #finalizeFracture(LVFracture)}.
     * @param table the table to create a new fracture
     * @return new Fracture object
     * @throws IOException
     */
    LVFracture createNewFracture(LVTable table) throws IOException;

    /**
     * Saves and finalizes the fracture definition.
     * A newly created fracture is inactive until this method call.
     * @param fracture the fracture object with full details (eg key range).
     * @throws IOException
     */
    void finalizeFracture(LVFracture fracture) throws IOException;
    
    /**
     * Deletes the fracture metadata object and related objects from this repository.
     * This is called only when some fractures are merged.
     * @param fracture the fracture object to delete
     * @throws IOException
     */
    void dropFracture (LVFracture fracture) throws IOException;

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
     * @return replica group objects. in ID order.
     * @throws IOException
     */
    LVReplicaGroup[] getAllReplicaGroups(int tableId) throws IOException;

    /**
     * Creates a new additional replica group in the given table with the specified partitioning column.
     * @param table the table to create a new group
     * @param partitioningColumn the partitioning column of the new group
     * @return new ReplicaGroup object
     * @throws IOException
     */
    LVReplicaGroup createNewReplicaGroup(LVTable table, LVColumn partitioningColumn) throws IOException;
    
    /**
     * Deletes the replica group metadata object and related objects from this repository.
     * The last replica group must not be deleted unless this is part of dropTable.
     * @param group the replica group object to delete
     * @throws IOException
     */
    void dropReplicaGroup (LVReplicaGroup group) throws IOException;

    //////////////////////// Rack Methods : begin ////////////////////////////////
    /**
     * Returns the rack object with the given ID. 
     * @param rackId Rack ID
     * @return rack object. null if the ID is not found.
     * @throws IOException
     */
    LVRack getRack (int rackId) throws IOException;

    /**
     * Returns the rack object with the given rack name. 
     * @param rackName name of the rack
     * @return rack object. null if the name is not found.
     * @throws IOException
     */
    LVRack getRack (String rackName) throws IOException;

    /**
     * Returns all rack objects. 
     * @return rack objects. in ID order.
     * @throws IOException
     */
    LVReplicaGroup[] getAllRacks() throws IOException;

    /**
     * Creates a new rack in the given table with the specified name.
     * @param name A unique name of the rack. Should be the same string as the rack names in HDFS.
     * @return new Rack object
     * @throws IOException
     */
    LVRack createNewRack(String name) throws IOException;

    /**
     * Changes the status of the given rack.
     * @param rack Rack object to be modified.
     * @param status new status of the rack.
     * @return Rack object after modification
     * @throws IOException
     */
    LVRack updateRackStatus(LVRack rack, RackStatus status) throws IOException;
    
    /**
     * Deletes the rack object and related objects from this repository.
     * @param rack the rack object to delete
     * @throws IOException
     */
    void dropRack (LVRack rack) throws IOException;

    //////////////////////// RackNode Methods : begin ////////////////////////////////

    /**
     * Returns the rack node object with the given ID. 
     * @param nodeId Node ID
     * @return rack node object. null if the ID is not found.
     * @throws IOException
     */
    LVRackNode getRackNode(int nodeId) throws IOException;

    /**
     * Returns the rack node object with the given name. 
     * @param name unique name of the node. probably FQDN.
     * @return rack node object. null if the name is not found.
     * @throws IOException
     */
    LVRackNode getRackNode(String nodeName) throws IOException;

    /**
     * Returns all rack nodes in the rack. 
     * @param rackId Rack ID.
     * @return rack node objects. in ID order.
     * @throws IOException
     */
    LVRackNode[] getAllRackNodes(int rackId) throws IOException;

    /**
     * Creates a new rack node in the given rack with the specified name.
     * @param rack the rack to which the new node will belong
     * @param name unique name of the new node. probably FQDN.
     * @return new RackNode object
     * @throws IOException
     */
    LVRackNode createNewRackNode(LVRack rack, String name) throws IOException;

    /**
     * Changes the status of the given node.
     * @param node Node object to be modified.
     * @param status new status of the node.
     * @return Node object after modification
     * @throws IOException
     */
    LVRackNode updateRackNodeStatus(LVRackNode node, RackNodeStatus status) throws IOException;
    
    /**
     * Deletes the rack node metadata object and related objects from this repository.
     * @param node the node object to delete
     * @throws IOException
     */
    void dropRackNode (LVRackNode node) throws IOException;

    //////////////////////// RackAssignment Methods : begin ////////////////////////////////

    /**
     * Returns the rack assignment object with the given ID. 
     * @param assignmentId Assignment ID
     * @return rack assignment object. null if the ID is not found.
     * @throws IOException
     */
    LVRackAssignment getRackAssignment(int assignmentId) throws IOException;

    /**
     * Returns the assignment object for the given rack and fracture. 
     * @param rack specified Rack.
     * @param fracture specified Fracture.
     * @return rack assignment object. null if not assigned yet.
     * @throws IOException
     */
    LVRackAssignment getRackAssignment(LVRack rack, LVFracture fracture) throws IOException;

    /**
     * Returns all rack assignments of the rack. 
     * @param rackId Rack ID.
     * @return rack assignment objects. in ID order.
     * @throws IOException
     */
    LVRackAssignment[] getAllRackAssignmentsByRack(int rackId) throws IOException;
    /**
     * Returns all rack assignments of the fracture. 
     * @param fractureId Fracture ID.
     * @return rack assignment objects. in ID order.
     * @throws IOException
     */
    LVRackAssignment[] getAllRackAssignmentsByFracture(int fractureId) throws IOException;

    /**
     * Creates a new rack assignment.
     * @param rack the rack to assign
     * @param fracture the fracture this assignment regards to
     * @param owner the replica group that will exclusively own the rack regarding to the fracture 
     * @return new RackAssignment object
     * @throws IOException
     */
    LVRackAssignment createNewRackAssignment(LVRack rack, LVFracture fracture, LVReplicaGroup owner) throws IOException;

    /**
     * Changes the owner of the given assignment.
     * @param assignment Assignment object to be modified.
     * @param owner new owner of the assignment.
     * @return Assignment object after modification
     * @throws IOException
     */
    LVRackAssignment updateRackAssignmentOwner(LVRackAssignment assignment, LVReplicaGroup owner) throws IOException;
    
    /**
     * Deletes the rack assignment metadata object.
     * @param assignment the assignment object to delete
     * @throws IOException
     */
    void dropRackAssignment (LVRackAssignment assignment) throws IOException;

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
     * @return replica scheme objects. in ID order.
     * @throws IOException
     */
    LVReplicaScheme[] getAllReplicaSchemes(int groupId) throws IOException;

    /**
     * Creates a new additional replica group in the given table
     * with the specified in-block sorting column and compression scheme.
     * Column compression scheme can be changed later.
     * Call {@link #changeColumnCompressionScheme(LVReplicaScheme, LVColumn, CompressionType)}
     * afterwards to do so.
     * @param group the replica group to create a replica scheme
     * @param sortingColumn the in-block sorting column of the replica scheme
     * @param columnCompressionSchemes compression schemes of each column (key=Column Id)
     * omitted columns are considered as no-compression.
     * @return new ReplicaScheme object
     * @throws IOException
     */
    LVReplicaScheme createNewReplicaScheme(LVReplicaGroup group, LVColumn sortingColumn,
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
                    LVColumn column, CompressionType compressionType) throws IOException;
    
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
     * @return replica objects. in ID order.
     * @throws IOException
     */
    LVReplica[] getAllReplicasBySchemeId(int schemeId) throws IOException;

    /**
     * Returns all replicas of the given fracture.
     * @param fractureId Fracture the replicas are based on.
     * @return replica objects. in ID order.
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
    LVReplica createNewReplica(LVReplicaScheme scheme, LVFracture fracture) throws IOException;

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
    LVSubPartitionScheme getSubPartitionScheme(int subPartitionSchemeId) throws IOException;

    /**
     * Returns all sub-partition scheme objects  in the given fracture.
     * @param fractureId Fracture the replica partition schemes are for.
     * @return sub-partition scheme objects. in ID order.
     * @throws IOException
     */
    LVSubPartitionScheme[] getAllSubPartitionSchemesByFractureId(int fractureId) throws IOException;

    /**
     * Returns all sub-partition scheme objects  of the given replica group.
     * @param groupId Replica Group the partition schemes are for.
     * @return sub-partition scheme objects. in ID order.
     * @throws IOException
     */
    LVSubPartitionScheme[] getAllSubPartitionSchemesByGroupId(int groupId) throws IOException;

    /**
     * Returns the sub-partition scheme object for the given . 
     * @param fractureId Fracture the sub-partition scheme is for.
     * @param groupId Replica Group the sub-partition scheme is for.
     * @return sub-partition scheme. null if the ID is not found.
     * @throws IOException
     */
    LVSubPartitionScheme getSubPartitionSchemeByFractureAndGroup(int fractureId, int groupId) throws IOException;

    /**
     * Creates a new sub-partition scheme for the given fracture and replica group.
     * As of this method call, the caller doesn't have to know the exact key range.
     * So, this method merely acquires a unique ID for the new sub-partition scheme.
     * As soon as the key ranges are finalized, call
     * {@link #finalizeSubPartitionScheme(LVSubPartitionScheme)}.
     * @param fracture Fracture the replica partition scheme is for.
     * @param group Replica Group the partition scheme is for.
     * @return new sub-partition scheme
     * @throws IOException
     */
    LVSubPartitionScheme createNewSubPartitionScheme(
                    LVFracture fracture, LVReplicaGroup group) throws IOException;

    /**
     * Saves and finalizes the sub-partition scheme definition.
     * A newly created sub-partition scheme is inactive until this method call.
     * @param subPartitionScheme the sub-partition scheme object with full details (eg key ranges).
     * @throws IOException
     */
    void finalizeSubPartitionScheme(LVSubPartitionScheme subPartitionScheme) throws IOException;
    
    /**
     * Deletes the sub-partition scheme metadata object.
     * This method does not delete subsequent objects because the deletion of containing fracture/group
     * will do it instead (fracture/group -> replica, subPartitionScheme -> subPartition is a lattice relationship).
     * @param subPartitionScheme the sub-partition scheme object to delete
     * @throws IOException
     */
    void dropSubPartitionScheme (LVSubPartitionScheme subPartitionScheme) throws IOException;

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
     * @param range key range the sub-partitions stores. index of {@link LVSubPartitionScheme#getRanges()}
     * @return sub-partition object.
     * @throws IOException
     */
    LVReplicaPartition getReplicaPartitionByReplicaAndRange(int replicaId, int range) throws IOException;

    /**
     * Creates a new sub-partition for the replica and range.
     * @param replica Replica (Replicated-Fracture) the sub-partition belongs to.
     * @param range key range the sub-partitions stores. index of {@link LVSubPartitionScheme#getRanges()}
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
     * @param checksum CRC32 checksum of the file
     * @return new column file
     * @throws IOException
     */
    LVColumnFile createNewColumnFile(LVReplicaPartition subPartition, LVColumn column,
                    String hdfsFilePath, long fileSize, int checksum) throws IOException;
    
    /**
     * Deletes the column file metadata object from this repository.
     * @param columnFile the column file object to delete
     * @throws IOException
     */
    void dropColumnFile (LVColumnFile columnFile) throws IOException;
}
