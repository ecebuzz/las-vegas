package edu.brown.lasvegas.util;

import static org.junit.Assert.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.client.DataNodeFile;
import edu.brown.lasvegas.client.LVDataClient;
import edu.brown.lasvegas.lvfs.LVFSFilePath;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.DataEngine;
import edu.brown.lasvegas.lvfs.local.LocalVirtualFile;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.server.LVDataNode;

/**
 * Testcase for {@link VirtualFileUtil}.
 */
public class VirtualFileUtilTest {
    @Test
    public void testCopyFileLocalToLocal () throws Exception {
        testCopyFileInternal(false, false);
    }
    /* output to remote is not supported yet
    @Test
    public void testCopyFileLocalToRemote () throws Exception {
        testCopyFileInternal(false, true);
    }
    */
    @Test
    public void testCopyFileRemoteToLocal () throws Exception {
        testCopyFileInternal(true, false);
    }
    /* output to remote is not supported yet
    @Test
    public void testCopyFileRemoteToRemote () throws Exception {
        testCopyFileInternal(true, true);
    }
    */

    private void testCopyFileInternal (boolean srcRemote, boolean destRemote) throws Exception {
        File directSrc;
        VirtualFile srcVirtualFile;
        if (srcRemote) {
            directSrc = new File (tmpDir, "src");
            srcVirtualFile = new DataNodeFile(dataClient.getChannel(), directSrc.getAbsolutePath());
        } else {
            directSrc = new File ("test/local", "src");
            srcVirtualFile = new LocalVirtualFile(directSrc);
        }
        createSrcFile (directSrc);
        
        File directDest;
        VirtualFile destVirtualFile;
        if (destRemote) {
            directDest = new File (tmpDir, "dest");
            destVirtualFile = new DataNodeFile(dataClient.getChannel(), directDest.getAbsolutePath());
        } else {
            directDest = new File ("test/local", "dest");
            destVirtualFile = new LocalVirtualFile(directDest);
        }
        if (directDest.exists()) {
            directDest.delete();
        }
        if (!directDest.getParentFile().exists()) {
            boolean created = directDest.getParentFile().mkdirs();
            assert (created);
        }
        
        VirtualFileUtil.copyFile(srcVirtualFile, destVirtualFile);
        
        validateDestFile (directDest);
    }
    private void createSrcFile (File srcFile) throws IOException {
        if (srcFile.exists()) {
            srcFile.delete();
        }
        if (!srcFile.getParentFile().exists()) {
            boolean created = srcFile.getParentFile().mkdirs();
            assert (created);
        }
        FileOutputStream out = new FileOutputStream(srcFile);
        byte[] bytes = new byte[1 << 16];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte) (i & 0xFF);
        }
        out.write(bytes);
        out.flush();
        out.close();
    }
    private void validateDestFile (File destFile) throws IOException {
        assert (destFile.exists());
        assertEquals (1 << 16, destFile.length());
        FileInputStream in = new FileInputStream(destFile);
        byte[] bytes = new byte[1 << 16];
        in.read(bytes);
        for (int i = 0; i < bytes.length; ++i) {
            assertEquals((byte) (i & 0xFF), bytes[i]);
        }
        in.close();
    }
    
    private static final String TEST_BDB_HOME = "test/dummy_bdb";
    private static final String DATANODE_ADDRESS = "localhost:42345";

    private static MasterMetadataRepository masterRepository;
    private static String rootDir;
    private static String tmpDir;
    private static LVDataNode dataNode;
    private static LVDataClient dataClient;
    private static Configuration conf;

    private static LVRack rack;
    private static LVRackNode node;

    @BeforeClass
    public static void setUpBeforeClass () throws Exception {
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
        rack = masterRepository.createNewRack("rack");
        node = masterRepository.createNewRackNode(rack, "node", DATANODE_ADDRESS);
        
        conf = new Configuration();
        rootDir = "test/node_lvfs_" + Math.abs(new Random(System.nanoTime()).nextInt());
        tmpDir = rootDir + "/tmp";
        conf.set(DataEngine.LOCA_LVFS_ROOTDIR_KEY, rootDir);
        conf.set(DataEngine.LOCA_LVFS_TMPDIR_KEY, tmpDir);
        conf.set(LVFSFilePath.LVFS_CONF_ROOT_KEY, rootDir);
        conf.set(LVDataNode.DATA_ADDRESS_KEY, DATANODE_ADDRESS);
        conf.set(LVDataNode.DATA_NODE_NAME_KEY, "node");
        conf.set(LVDataNode.DATA_RACK_NAME_KEY, "rack");
        dataNode = new LVDataNode(conf, masterRepository);
        dataNode.start(null);
        dataClient = new LVDataClient(conf, node.getAddress());
    }
    @AfterClass
    public static void tearDownAfterClass () throws Exception {
        dataClient.release();
        dataNode.close();
        masterRepository.shutdown();
    }
}
