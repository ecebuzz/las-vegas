package edu.brown.lasvegas.lvfs.data;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.log4j.Logger;

import edu.brown.lasvegas.protocol.LVDataProtocol;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Implementation of {@link LVDataProtocol}.
 */
public final class DataEngine implements LVDataProtocol, Closeable, Configurable {
    private static Logger LOG = Logger.getLogger(DataEngine.class);

    public static final String LOCA_LVFS_ROOTDIR_KEY = "lasvegas.server.data.local.rootdir";
    public static final String LOCA_LVFS_ROOTDIR_DEFAULT= "lvfs_localdir";

    public static final String LOCA_LVFS_TMPDIR_KEY = "lasvegas.server.data.local.tmpdir";
    public static final String LOCA_LVFS_TMPDIR_DEFAULT= "lvfs_localdir/tmp";

    private DataEngineContext context;
    /** The thread to continuously pull new tasks for the node. */
    private final DataTaskPollingThread pollingThread;
    
    public DataEngine (LVMetadataProtocol metaRepo, int nodeId) throws IOException {
        this (metaRepo, nodeId, new Configuration());
    }
    public DataEngine (LVMetadataProtocol metaRepo, int nodeId, Configuration conf) throws IOException {
        assert (metaRepo != null);
        assert (conf != null);
        context = new DataEngineContext();
        context.metaRepo = metaRepo;
        context.conf = conf;
        context.nodeId = nodeId;
        context.localLvfsRootDir = getLvfsDir (conf.get(LOCA_LVFS_ROOTDIR_KEY, LOCA_LVFS_ROOTDIR_DEFAULT));
        context.localLvfsTmpDir = getLvfsDir (conf.get(LOCA_LVFS_TMPDIR_KEY, LOCA_LVFS_TMPDIR_DEFAULT));
        this.pollingThread = new DataTaskPollingThread(context);
    }
    private File getLvfsDir (String path) throws IOException {
        File file = new File (path);
        if (!file.exists()) {
            boolean created = file.mkdirs();
            if (!created) {
                LOG.error("failed to create an LVFS directory (" + file.getAbsolutePath() + ") for local LVFS.");
                throw new IOException ("failed to create an LVFS directory (" + file.getAbsolutePath() + ") for local LVFS.");
            } else {
                LOG.info("created an empty local LVFS directory: " + file.getAbsolutePath());
            }
        }
        return file;
    }
    
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        if (protocol.equals(LVDataProtocol.class.getName())) {
            return LVDataProtocol.versionID;
        } else {
            throw new IOException("This protocol is not supported: " + protocol);
        }
    }

    /** start running the data node. */
    public void start() {
        pollingThread.start();
    }

    @Override
    public void shutdown() throws IOException {
        pollingThread.shutdown();
        try {
            pollingThread.join();
        } catch (InterruptedException ex) {
        }
    }
    
    @Override
    public void close() throws IOException {
        shutdown();
    }
    
    @Override
    public byte[] getFileBody(String localPath, int offset, int len) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public int getFileLength(String localPath) throws IOException {
        File file = new File (localPath);
        assert (file.length() <= 0x7FFFFFFF);
        return (int) file.length();
    }
    @Override
    public boolean existsFile(String localPath) throws IOException {
        return new File (localPath).exists();
    }
    @Override
    public boolean isDirectory(String localPath) throws IOException {
        return new File (localPath).isDirectory();
    }
    @Override
    public int[] getCombinedFileStatus(String localPath) throws IOException {
        File file = new File (localPath);
        assert (file.length() <= 0x7FFFFFFF);
        return new int[]{(int) file.length(), file.exists() ? 1 : 0, file.isDirectory() ? 1 : 0};
    }
    @Override
    public boolean deleteFile(String localPath, boolean recursive) throws IOException {
        if (!localPath.startsWith(context.localLvfsRootDir.getAbsolutePath())) {
            throw new IOException ("this file seems not part of LVFS. deletion refused: " + localPath);
        }
        File file = new File (localPath);
        if (!file.exists()) {
            return false;
        }
        if (!recursive || !file.isDirectory()) {
            return file.delete();
        }
        return deleteFileRecursive (file);
    }
    private final static String SAFE_DIR_PREFIX = "/home/hkimura/workspace/las-vegas/lvfs/test/";
    private boolean deleteFileRecursive (File dir) throws IOException {
        // TODO this additional check is a tentative code. will be removed when I become really confident
        if (!dir.getAbsolutePath().startsWith(SAFE_DIR_PREFIX)) {
            throw new IOException ("wait, wait! you are going to recursively delete " + dir.getAbsolutePath());
        }
        
        if (!dir.isDirectory()) {
            return dir.delete();
        }
        for (File child : dir.listFiles()) {
            if (!deleteFileRecursive(child)) {
                return false;
            }
        }
        return dir.delete();
    }
    
    @Override
    public Configuration getConf() {
        return context.conf;
    }
    @Override
    public void setConf(Configuration conf) {
        context.conf = conf;
    }
}
