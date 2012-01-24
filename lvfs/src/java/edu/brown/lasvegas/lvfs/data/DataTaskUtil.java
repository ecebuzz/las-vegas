package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplica;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.ColumnFileBundle;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.LVFSFileType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;

/**
 * Misc methods used from a few tasks and jobs in this package.
 */
public class DataTaskUtil {
    /**
     * Registers the given temporary files as new LVColumnFile record in the metadata.
     * This method moves the columnar files to non-temporary folder.
     * Also it renames the files according to the rule defined in {@link LVFSFilePath}.
     */
    public static void registerTemporaryFilesAsColumnFiles (
                    DataEngineContext context,
                    LVReplicaPartition partition,
                    LVColumn[] columns,
                    ColumnFileBundle[] temporaryFiles) throws IOException {
        assert (columns.length == temporaryFiles.length);
        assert (partition.getNodeId() == context.nodeId); // this must be a local task
        final int partitionId = partition.getPartitionId();
        LVReplica replica = context.metaRepo.getReplica(partition.getReplicaId());
        LVReplicaScheme scheme = context.metaRepo.getReplicaScheme(replica.getSchemeId());
        LVFracture fracture = context.metaRepo.getFracture(replica.getFractureId());
        LVTable table = context.metaRepo.getTable(fracture.getTableId());
        for (int i = 0; i < columns.length; ++i) {
            assert (columns[i].getTableId() == table.getTableId());
            assert (temporaryFiles[i] != null);
            final int columnId = columns[i].getColumnId();
            // if a record already exists, drop it.
            LVColumnFile existingFile = context.metaRepo.getColumnFileByReplicaPartitionAndColumn(partitionId, columnId);
            if (existingFile != null) {
                context.metaRepo.dropColumnFile(existingFile.getColumnFileId());
            }
            ColumnFileBundle tmpFile = temporaryFiles[i];

            // first, create an LVColumnFile record for the file. At this point we have no idea what the ID is, so we can't construct the permanent path.
            int columnFileId = context.metaRepo.createNewColumnFileIdOnlyReturn(partitionId, columnId,
                "", // this value is bogus at this point
                // but other values are final
                (int) tmpFile.getDataFile().length(), tmpFile.getTupleCount(), tmpFile.getDataFileChecksum(),
                tmpFile.getDictionaryBytesPerEntry(), tmpFile.getDistinctValues(), tmpFile.getRunCount(), tmpFile.getUncompressedSizeKB());
            
            // then, construct the permanent path
            String pathWithoutExtension = new LVFSFilePath(
                context.conf, table.getDatabaseId(), table.getTableId(), fracture.getFractureId(), scheme.getSchemeId(),
                partition.getRange(), partitionId, columnId, columnFileId, null).getAbsolutePath();
            // move the files
            moveFile (tmpFile.getDataFile(), pathWithoutExtension, LVFSFileType.DATA_FILE);
            moveFile (tmpFile.getDictionaryFile(), pathWithoutExtension, LVFSFileType.DICTIONARY_FILE);
            moveFile (tmpFile.getPositionFile(), pathWithoutExtension, LVFSFileType.POSITION_FILE);
            moveFile (tmpFile.getValueFile(), pathWithoutExtension, LVFSFileType.VALUE_FILE);
            // finally update the LVColumnFile record
            context.metaRepo.updateColumnFilePathNoReturn(columnFileId, pathWithoutExtension);
        }
    }
    
    private static void moveFile (VirtualFile tmpFile, String pathWithoutExtension, LVFSFileType type) throws IOException {
        if (tmpFile == null) {
            return;
        }
        assert (tmpFile instanceof LocalVirtualFile);
        assert (tmpFile.exists()); // if it's non-null but not-exists, we failed to create some file
        LocalVirtualFile newPath = new LocalVirtualFile(type.appendExtension(pathWithoutExtension));
        if (!tmpFile.renameTo(newPath)) {
            throw new IOException ("failed to move temporary file " + tmpFile + " file to new place " + newPath);
        }
    }

}
