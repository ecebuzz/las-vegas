package edu.brown.lasvegas.lvfs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVColumnFile;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVReplicaPartition;
import edu.brown.lasvegas.LVReplicaScheme;
import edu.brown.lasvegas.LVTable;

/**
 * Represents a path of a columnar file in LVFS.
 * This class defines the rules to determine the path of columnar files. 
 * <p>The path of a columnar file consists of following parts:
 * PREFIX, TABLE_ID, FRACTURE_ID"_"REPLICA_SCHEME_ID, RANGE"_"REPLICA_PARTITION_ID,
 * COLUMN_ID"_"COLUMN_FILE_ID, types of files (data, position, dictionary).</p>
 * 
 * <p>Examples, "/lvfs/22/30_143/0_111/333_2344.dat", "/lvfs/22/30_143/1_112/333_4343.dic"</p>
 */
public final class LVFSFilePath {
    /** name of the configuration value which defines the root folder path of LVFS. */
    public final static String LVFS_CONF_ROOT_KEY = "lasvegas.lvfs.rootdir";
    
    /** full path of the file. */
    private final String absolutePath;
    /** the root folder path of LVFS. Should be the value defined as "lasvegas.lvfs.rootdir" in configuration files.*/
    private final String lvfsRootDir;
    /** ID of {@link LVTable}. */
    private final int tableId;
    /** ID of {@link LVFracture}. */
    private final int fractureId;
    /** ID of {@link LVReplicaScheme}. */
    private final int replicaSchemeId;
    /** Index of the stored range (partition). See {@link LVReplicaPartition#getRange()}.*/
    private final int range;
    /** ID of {@link LVReplicaPartition}.*/
    private final int replicaPartitionId;
    /** ID of {@link LVColumn}.*/
    private final int columnId;
    /** ID of {@link LVColumnFile}.*/
    private final int columnFileId;
    /** type of the file. */
    private final LVFSFileType type;

    /**
     * Constructs a file path from IDs of metadata objects.
     * @param lvfsRootDir the root folder path of LVFS. Should be the value defined as "lasvegas.lvfs.rootdir" in configuration files
     * @param tableId ID of {@link LVTable}
     * @param fractureId ID of {@link LVFracture}
     * @param replicaSchemeId ID of {@link LVReplicaScheme}
     * @param range Index of the stored range (partition). See {@link LVReplicaPartition#getRange()}
     * @param replicaPartitionId ID of {@link LVReplicaPartition}
     * @param columnId ID of {@link LVColumn}
     * @param columnFileId ID of {@link LVColumnFile}
     * @param type type of the file
     */
    public LVFSFilePath(String lvfsRootDir, int tableId, int fractureId, int replicaSchemeId, int range, int replicaPartitionId, int columnId, int columnFileId, LVFSFileType type) {
        assert (lvfsRootDir.indexOf("://") < 0); // protocol part should have been removed beforehand
        assert (lvfsRootDir.endsWith("/")); // lasvegas.lvfs.rootdir should always end with /
        assert (range >= 0);
        this.lvfsRootDir = lvfsRootDir;
        this.tableId = tableId;
        this.fractureId = fractureId;
        this.replicaSchemeId = replicaSchemeId;
        this.range = range;
        this.replicaPartitionId = replicaPartitionId;
        this.columnId = columnId;
        this.columnFileId = columnFileId;
        this.type = type;
        this.absolutePath = lvfsRootDir + tableId + "/"
            + fractureId + "_" + replicaSchemeId + "/"
            + range + "_" + replicaPartitionId + "/"
            + columnId + "_" + columnFileId + "." + type.getExtension();
    }

    /**
     * Overload to use root folder defined in the configuration file.
     */
    public LVFSFilePath(Configuration conf, int tableId, int fractureId, int replicaSchemeId, int range, int replicaPartitionId, int columnId, int columnFileId, LVFSFileType type) throws IOException {
        this(getLvfsRootDirFromConf(conf), tableId, fractureId, replicaSchemeId, range, replicaPartitionId, columnId, columnFileId, type);
    }
    private static String getLvfsRootDirFromConf (Configuration conf) throws IOException {
        String dir = conf.get(LVFS_CONF_ROOT_KEY);
        if (dir == null) {
            throw new IOException (LVFS_CONF_ROOT_KEY + " is not specified in configuration file");
        }
        return dir.endsWith("/") ? dir : dir + "/";
    }

    private static final Pattern pattern;
    static {
        pattern = Pattern.compile("(.*/)([0-9]+)/([0-9]+)_([0-9]+)/([0-9]+)_([0-9]+)/([0-9]+)_([0-9]+)\\.([a-z]+)");
    }
    /**
     * Extracts IDs of metadata objects from a file path.
     * @param absolutePath a full path of a columnar file in LVFS.
     * @throws Exception if the given path is not a legitimate file path.
     */
    public LVFSFilePath(String absolutePath) throws IOException {
        this.absolutePath = absolutePath;
        Matcher matcher = pattern.matcher(absolutePath);
        if (!matcher.matches()) {
            throw new IOException("Not a valid file path in LVFS:" + absolutePath);
        }
        String extension;
        try {
            int i = 0;
            this.lvfsRootDir = matcher.group(++i);
            this.tableId = Integer.parseInt(matcher.group(++i));
            this.fractureId = Integer.parseInt(matcher.group(++i));
            this.replicaSchemeId = Integer.parseInt(matcher.group(++i));
            this.range = Integer.parseInt(matcher.group(++i));
            this.replicaPartitionId = Integer.parseInt(matcher.group(++i));
            this.columnId = Integer.parseInt(matcher.group(++i));
            this.columnFileId = Integer.parseInt(matcher.group(++i));
            extension = matcher.group(++i);
        } catch (Exception ex) {
            throw new IOException("Failed to parse a file path:" + absolutePath, ex);
        }
        this.type = LVFSFileType.getFromExtension(extension);
        if (type == null) {
            throw new IOException("Unrecognized extension:" + extension + ", path=" + absolutePath);
        }
    }
    
    @Override
    public String toString() {
        return absolutePath + " - ("
        + "lvfsRootDir=" + lvfsRootDir
        + ", tableId=" + tableId
        + ", fractureId=" + fractureId
        + ", replicaSchemeId=" + replicaSchemeId
        + ", range=" + range
        + ", replicaPartitionId=" + replicaPartitionId
        + ", columnId=" + columnId
        + ", columnFileId=" + columnFileId
        + ", type=" + type
        + ")";
    }
    
    @Override
    public int hashCode() {
        return absolutePath.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof LVFSFilePath)) {
            return false;
        }
        return ((LVFSFilePath) obj).absolutePath.equals(absolutePath);
    }

    /** Returns the path of the folder containing this file. */
    public String getParentFolderPath () {
        return absolutePath.substring(0, absolutePath.lastIndexOf('/'));
    }
    
    /**
     * Gets the full path of the file.
     *
     * @return the full path of the file
     */
    public String getAbsolutePath() {
        return absolutePath;
    }
    
    /**
     * Gets the root folder path of LVFS.
     *
     * @return the root folder path of LVFS
     */
    public String getLvfsRootDir() {
        return lvfsRootDir;
    }
    
    /**
     * Gets the iD of {@link LVTable}.
     *
     * @return the iD of {@link LVTable}
     */
    public int getTableId() {
        return tableId;
    }
    
    /**
     * Gets the iD of {@link LVFracture}.
     *
     * @return the iD of {@link LVFracture}
     */
    public int getFractureId() {
        return fractureId;
    }
    
    /**
     * Gets the iD of {@link LVReplicaScheme}.
     *
     * @return the iD of {@link LVReplicaScheme}
     */
    public int getReplicaSchemeId() {
        return replicaSchemeId;
    }
    
    /**
     * Gets the index of the stored range (partition).
     *
     * @return the index of the stored range (partition)
     */
    public int getRange() {
        return range;
    }
    
    /**
     * Gets the iD of {@link LVReplicaPartition}.
     *
     * @return the iD of {@link LVReplicaPartition}
     */
    public int getReplicaPartitionId() {
        return replicaPartitionId;
    }
    
    /**
     * Gets the iD of {@link LVColumn}.
     *
     * @return the iD of {@link LVColumn}
     */
    public int getColumnId() {
        return columnId;
    }
    
    /**
     * Gets the iD of {@link LVColumnFile}.
     *
     * @return the iD of {@link LVColumnFile}
     */
    public int getColumnFileId() {
        return columnFileId;
    }
    
    /**
     * Gets the type of the file.
     *
     * @return the type of the file
     */
    public LVFSFileType getType() {
        return type;
    }
}
