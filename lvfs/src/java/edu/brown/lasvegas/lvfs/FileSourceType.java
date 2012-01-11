package edu.brown.lasvegas.lvfs;

import java.io.IOException;

import edu.brown.lasvegas.lvfs.hdfs.HdfsVirtualFile;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.util.URLVirtualFile;

/**
 * Specifies what a file path means.
 */
public enum FileSourceType {
    /** a path in local filesystem. */
    LOCALFS {
        @Override
        public VirtualFile getFile(String path) throws IOException {
            return new LocalVirtualFile(path);
        }
    },
    /** a path in (possibly remote) LVFS. */
    LVFS {
        @Override
        public VirtualFile getFile(String path) throws IOException {
            // DataNodeFile needs data node name...
            // TODO lvfs://node1:12345/relative/path/here everywhere?
            throw new UnsupportedOperationException();
        }
    },
    /** a path in HDFS. */
    HDFS {
        @Override
        public VirtualFile getFile(String path) throws IOException {
            return new HdfsVirtualFile(path);
        }
    },
    /** a URL. */
    URL {
        @Override
        public VirtualFile getFile(String path) throws IOException {
            return new URLVirtualFile(new java.net.URL(path));
        }
    },

    /** kind of null. */
    INVALID {
        @Override
        public VirtualFile getFile(String path) throws IOException {
            throw new UnsupportedOperationException();
        }
    },
    
    ;
    
    /**
     * Instantiates a VirtualFile for the given path.
     */
    public abstract VirtualFile getFile (String path) throws IOException;
}
