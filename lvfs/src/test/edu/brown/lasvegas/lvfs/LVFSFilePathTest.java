package edu.brown.lasvegas.lvfs;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Testcases for {@link LVFSFilePath}.
 */
public class LVFSFilePathTest {
    @Test
    public void testExtract1 () throws Exception {
        LVFSFilePath path = new LVFSFilePath("/lvfs/22/30_143/0_111/333_2344.dat");
        assertEquals ("/lvfs/22/30_143/0_111/333_2344.dat", path.getAbsolutePath());
        assertEquals ("/lvfs/", path.getLvfsRootDir());
        assertEquals (22, path.getTableId());
        assertEquals (30, path.getFractureId());
        assertEquals (143, path.getReplicaSchemeId());
        assertEquals (0, path.getRange());
        assertEquals (111, path.getReplicaPartitionId());
        assertEquals (333, path.getColumnId());
        assertEquals (2344, path.getColumnFileId());
        assertEquals (LVFSFileType.DATA_FILE, path.getType());
        assertEquals ("/lvfs/22/30_143/0_111", path.getParentFolderPath());
    }

    @Test
    public void testExtract2 () throws Exception {
        LVFSFilePath path = new LVFSFilePath("/lvfs/22/30_143/1_112/333_4343.dic");
        assertEquals ("/lvfs/22/30_143/1_112/333_4343.dic", path.getAbsolutePath());
        assertEquals ("/lvfs/", path.getLvfsRootDir());
        assertEquals (22, path.getTableId());
        assertEquals (30, path.getFractureId());
        assertEquals (143, path.getReplicaSchemeId());
        assertEquals (1, path.getRange());
        assertEquals (112, path.getReplicaPartitionId());
        assertEquals (333, path.getColumnId());
        assertEquals (4343, path.getColumnFileId());
        assertEquals (LVFSFileType.DICTIONARY_FILE, path.getType());
        assertEquals ("/lvfs/22/30_143/1_112", path.getParentFolderPath());
    }

    @Test
    public void testConstruct () throws Exception {
        LVFSFilePath path = new LVFSFilePath("/lvfsrootdir/", 112, 43, 343, 3, 234, 34, 3443, LVFSFileType.POSITION_FILE);
        assertEquals ("/lvfsrootdir/112/43_343/3_234/34_3443.pos", path.getAbsolutePath());
        assertEquals ("/lvfsrootdir/", path.getLvfsRootDir());
        assertEquals (112, path.getTableId());
        assertEquals (43, path.getFractureId());
        assertEquals (343, path.getReplicaSchemeId());
        assertEquals (3, path.getRange());
        assertEquals (234, path.getReplicaPartitionId());
        assertEquals (34, path.getColumnId());
        assertEquals (3443, path.getColumnFileId());
        assertEquals (LVFSFileType.POSITION_FILE, path.getType());
        assertEquals ("/lvfsrootdir/112/43_343/3_234", path.getParentFolderPath());
    }

    @Test
    public void testConstructConf () throws Exception {
        for (int i = 0; i < 2; ++i) {
            Configuration conf = new Configuration();
            conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, "/assd" + (i == 0 ? "" : "/")); // also test the feature to complete the ending /
            LVFSFilePath path = new LVFSFilePath(conf, 112, 43, 343, 3, 234, 34, 3443, LVFSFileType.POSITION_FILE);
            assertEquals ("/assd/112/43_343/3_234/34_3443.pos", path.getAbsolutePath());
            assertEquals ("/assd/", path.getLvfsRootDir());
            assertEquals (112, path.getTableId());
            assertEquals (43, path.getFractureId());
            assertEquals (343, path.getReplicaSchemeId());
            assertEquals (3, path.getRange());
            assertEquals (234, path.getReplicaPartitionId());
            assertEquals (34, path.getColumnId());
            assertEquals (3443, path.getColumnFileId());
            assertEquals (LVFSFileType.POSITION_FILE, path.getType());
            assertEquals ("/assd/112/43_343/3_234", path.getParentFolderPath());
        }
    }
}
