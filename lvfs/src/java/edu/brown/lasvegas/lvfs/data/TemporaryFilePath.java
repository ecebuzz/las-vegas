package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.brown.lasvegas.CompressionType;

/**
 * Represents a name of a temporary file.
 * <nodeId>_<replicaGroupId>_<fractureId>_<partition>_<random(to make it unique)>.extension.
 */
public class TemporaryFilePath {
    private static final Pattern pattern;
    static {
        pattern = Pattern.compile("(.*/)([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)_[-]?([0-9]+)\\.([a-z]+)");
    }
    public TemporaryFilePath(String folderPath, int nodeId, int replicaGroupId, int fractureId, int partition, int uniquefier, CompressionType compression) {
        this.folderPath = folderPath;
        this.nodeId = nodeId;
        this.replicaGroupId = replicaGroupId;
        this.fractureId = fractureId;
        this.partition = partition;
        this.uniquefier = uniquefier;
        this.compression = compression;
    }
    private static String trimSl (String path) {
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        } else {
            return path;
        }
    }
    public TemporaryFilePath(String filePath) throws IOException {
        Matcher matcher = pattern.matcher(filePath);
        if (!matcher.matches()) {
            throw new IOException("Not a valid file path as an intermediate result file:" + filePath);
        }
        String extension;
        try {
            int i = 0;
            this.folderPath = trimSl(matcher.group(++i));
            this.nodeId = Integer.parseInt(matcher.group(++i));
            this.replicaGroupId = Integer.parseInt(matcher.group(++i));
            this.fractureId = Integer.parseInt(matcher.group(++i));
            this.partition = Integer.parseInt(matcher.group(++i));
            this.uniquefier = Integer.parseInt(matcher.group(++i));
            extension = matcher.group(++i);
        } catch (Exception ex) {
            throw new IOException("Failed to parse a file path:" + filePath, ex);
        }
        if (extension.equals("snappy")) {
            this.compression = CompressionType.SNAPPY;
        } else if (extension.equals("gz")) {
            this.compression = CompressionType.GZIP_BEST_COMPRESSION;
            
        } else {
            if (!extension.equals("txt")) {
                throw new IOException("Unexpected extension:" + filePath);
            }
            this.compression = CompressionType.NONE;
        }
    }
    public String getFilePath () {
        if (folderPath.length() == 0) {
            return getFileName();
        } else {
            return folderPath + "/" + getFileName();
        }
    }
    
    public String getFileName () {
        String extension;
        if (compression == CompressionType.SNAPPY) {
            extension = "snappy";
        } else if (compression == CompressionType.GZIP_BEST_COMPRESSION) {
            extension = "gz";
        } else {
            extension = "txt";
            assert (compression == CompressionType.NONE);
        }
        return nodeId + "_" + replicaGroupId + "_" + fractureId + "_" + partition + "_" + uniquefier + "." + extension;
    }
    public final String folderPath;
    public final int nodeId;
    public final int replicaGroupId;
    public final int fractureId;
    public final int partition;
    public final int uniquefier;
    public final CompressionType compression;
}