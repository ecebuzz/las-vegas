package edu.brown.lasvegas.lvfs.data;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;

import edu.brown.lasvegas.protocol.LVDataProtocol;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * Implementation of {@link LVDataProtocol}.
 */
public final class DataEngine implements LVDataProtocol, Closeable {
    private static Logger LOG = Logger.getLogger(DataEngine.class);

    public static final String LOCA_LVFS_ROOTDIR_KEY = "lasvegas.server.data.local.rootdir";
    public static final String LOCA_LVFS_ROOTDIR_DEFAULT= "lvfs_localdir";

    public static final String LOCA_LVFS_TMPDIR_KEY = "lasvegas.server.data.local.tmpdir";
    public static final String LOCA_LVFS_TMPDIR_DEFAULT= "lvfs_localdir/tmp";

    private DataEngineContext context;
    /** The thread to continuously pull new tasks for the node. */
    private final DataTaskPollingThread pollingThread;
    
    private boolean didShutdown = false;
    
    public DataEngine (LVMetadataProtocol metaRepo, int nodeId) throws IOException {
        this (metaRepo, nodeId, new Configuration());
    }
    public DataEngine (LVMetadataProtocol metaRepo, int nodeId, Configuration conf) throws IOException {
        this (metaRepo, nodeId, conf, false);
    }
    public DataEngine (LVMetadataProtocol metaRepo, int nodeId, Configuration conf, boolean format) throws IOException {
        assert (metaRepo != null);
        assert (conf != null);
        this.context = new DataEngineContext(nodeId, conf, metaRepo,
                getLvfsDir (conf.get(LOCA_LVFS_ROOTDIR_KEY, LOCA_LVFS_ROOTDIR_DEFAULT)),
                getLvfsDir (conf.get(LOCA_LVFS_TMPDIR_KEY, LOCA_LVFS_TMPDIR_DEFAULT)));
        if (format) {
            formatDataDir ();
        }
        this.pollingThread = new DataTaskPollingThread(context);
    }
    /** rename the existing data folder to cleanup everything. */
    private void formatDataDir () throws IOException {
        // we never delete the old folder. just rename.
        if (context.localLvfsRootDir.exists()) {
            File backup = new File(context.localLvfsRootDir.getParentFile(), context.localLvfsRootDir.getName() + "_backup_"
                + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) // append backup-date
                + "_" + Math.abs(new Random(System.nanoTime()).nextInt())); // to make it unique
            LOG.info("renaming the existing data folder to " + backup.getAbsolutePath());
            boolean renamed = context.localLvfsRootDir.renameTo(backup);
            if (!renamed) {
                throw new IOException ("failed to backup the old data folder:" + context.localLvfsRootDir.getAbsolutePath());
            }
            LOG.info("renamed as a backup");
            getLvfsDir(context.localLvfsRootDir.getAbsolutePath());
            getLvfsDir(context.localLvfsTmpDir.getAbsolutePath());
        }
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
        didShutdown = true;
    }
    public boolean isShutdown () {
        return didShutdown;
    }
    
    @Override
    public void close() throws IOException {
        shutdown();
    }

    @Override
    public RemoteInputStream getFileInputStream(String localPath) throws IOException {
        File file = new File (localPath);
        if (!file.exists()) {
            throw new FileNotFoundException(localPath + " doesn't exist");
        }
        if (file.isDirectory()) {
            throw new IOException(localPath + " is a directory");
        }

        RemoteInputStreamServer istream = null;
        try {
            // we do NOT buffer in this layer. It's caller's responsibility to access by reasonably large chunk.
            // we can save memory consumption and additional overhead instead.
            istream = new SimpleRemoteInputStream(new FileInputStream(file));
            RemoteInputStream result = istream.export();
            istream = null;
            return result;
        } finally {
            if (istream != null) {
                istream.close();
            }
        }
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
    private boolean deleteFileRecursive (File dir) throws IOException {
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
    
    public Configuration getConf() {
        return context.conf;
    }
}
