package edu.brown.lasvegas.lvfs.data;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

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
        File file = new File (localPath);
        if (!file.exists()) {
            throw new FileNotFoundException(localPath + " doesn't exist");
        }
        if (file.isDirectory()) {
            throw new IOException(localPath + " is a directory");
        }
        if (offset >= file.length()) {
            throw new IOException("out of bounds. " + localPath + " has only " + file.length() + " bytes");
        }
        if (file.length() > 0x7FFFFFFF) {
            throw new IOException(localPath + " is too large");
        }
        if (offset + len > file.length()) {
            len = (int) file.length() - offset;
        }
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(offset);
        byte[] bytes = new byte[len];
        int read = raf.read(bytes);
        assert (read == len);
        return bytes;
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
        File file = new File (localPath);
        return file.exists() && file.isDirectory();
    }
    @Override
    public int[] getCombinedFileStatus(String localPath) throws IOException {
        File file = new File (localPath);
        if (!file.exists()) {
            return new int[]{0, 0, 0};
        }
        if (file.isDirectory()) {
            return new int[]{0, 1, 1};
        }
        assert (file.length() <= 0x7FFFFFFF);
        return new int[]{(int) file.length(), 1, 0};
    }
    @Override
    public boolean deleteFile(String localPath, boolean recursive) throws IOException {
        File file = new File (localPath);
        if (!file.getAbsolutePath().startsWith(context.localLvfsRootDir.getAbsolutePath())) {
            throw new IOException ("this file seems not part of LVFS. deletion refused: " + file.getAbsolutePath() + ". rootdir=" + context.localLvfsRootDir.getAbsolutePath());
        }
        if (!file.exists()) {
            return false;
        }
        if (!recursive || !file.isDirectory()) {
            return file.delete();
        }
        return deleteFileRecursive (file);
    }
    private final static String SAFE_DIR_PREFIX = "/home/hkimura/workspace/las-vegas/lvfs/test/";
    private final static String SAFE_DIR_PREFIX2 = "/var/lib/jenkins/jobs/lvfs/";
    private boolean deleteFileRecursive (File dir) throws IOException {
        // TODO this additional check is a tentative code. will be removed when I become really confident
        if (!dir.getAbsolutePath().startsWith(SAFE_DIR_PREFIX) && !dir.getAbsolutePath().startsWith(SAFE_DIR_PREFIX2)) {
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
