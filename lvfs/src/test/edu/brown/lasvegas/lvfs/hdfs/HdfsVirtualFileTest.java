package edu.brown.lasvegas.lvfs.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * Testcases for {@link HdfsVirtualFile}.
 */
public class HdfsVirtualFileTest {
    private MiniDFSCluster dfsCluster = null;

    // TODO these should be initialized once and for all to reduce testcase runtime.
    @Before
    public void setUp() throws Exception {
        dfsCluster = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(2)
            .format(true).racks(null).build();
    }
    
    @After
    public void tearDown() throws Exception {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
            dfsCluster = null;
        }
    }

    @Test
    public void testCreate () throws IOException {
        HdfsVirtualFile file = new HdfsVirtualFile(dfsCluster.getFileSystem(), new Path("test.txt"));
        assertEquals("test.txt", file.getName());
        VirtualFileOutputStream out = file.getOutputStream();
        final String str = "lkjsalkdjlkajsdlkjasd";
        out.write(str.getBytes("UTF-8"));
        out.flush();
        out.syncDurable();
        out.close();
        assertEquals(str.length(), file.length());
        assertTrue(file.exists());
        assertTrue(file.delete());
    }

    @Test
    public void testFolders () throws IOException {
        HdfsVirtualFile file = new HdfsVirtualFile(dfsCluster.getFileSystem(), new Path("/f1/f2/f3/test.txt"));
        assertTrue(file.getParentFile().mkdirs());
        assertEquals("test.txt", file.getName());
        assertFalse(file.isDirectory());
        assertEquals("/f1/f2/f3/test.txt", file.getAbsolutePath());

        VirtualFile f3 = file.getParentFile();
        assertEquals("f3", f3.getName());
        assertTrue(f3.isDirectory());
        assertEquals("/f1/f2/f3", f3.getAbsolutePath());
        assertTrue(f3.exists());

        VirtualFile f2 = f3.getParentFile();
        assertEquals("f2", f2.getName());
        assertTrue(f2.isDirectory());
        assertEquals("/f1/f2", f2.getAbsolutePath());
        assertTrue(f2.exists());

        VirtualFile f1 = f2.getParentFile();
        assertEquals("f1", f1.getName());
        assertTrue(f1.isDirectory());
        assertEquals("/f1", f1.getAbsolutePath());
        assertTrue(f1.exists());

        VirtualFile f0 = f1.getParentFile();
        assertEquals("/", f0.getAbsolutePath());
        assertTrue(f0.isDirectory());
        assertTrue(f0.exists());

        assertNull(f0.getParentFile());
    }
}
