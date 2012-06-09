package edu.brown.lasvegas.server;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.ClearAllTest;
import edu.brown.lasvegas.LVRack;
import edu.brown.lasvegas.LVRackNode;
import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;

/**
 * Testcase for {@link PreregisterDataNodes}.
 */
public class PreregisterDataNodesTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static final Logger LOG = Logger.getLogger(PreregisterDataNodesTest.class);
    private static final File listFile = new File ("src/test/edu/brown/lasvegas/server/datanodes_test.txt");
    private MasterMetadataRepository masterRepository;

    @Before
    public void setUp () throws IOException {
        ClearAllTest.deleteFileRecursive(new File("test"));
        if (!listFile.exists()) {
            throw new FileNotFoundException(listFile.getAbsolutePath() + " doesn't exist.");
        }
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME);
    }
    
    @After
    public void tearDown () throws IOException {
        masterRepository.shutdown();
    }
    
    @Test
    public void testAll () throws Exception {
        LOG.info("pre-registering data nodes..");
        PreregisterDataNodes.execute(masterRepository, listFile.getAbsolutePath());

        LVRack[] racks = masterRepository.getAllRacks();
        assertEquals (1, racks.length);
        assertEquals ("rack1", racks[0].getName());

        LVRackNode[] nodes = masterRepository.getAllRackNodes(racks[0].getRackId());
        assertEquals (2, nodes.length);
        assertEquals ("poseidon", nodes[0].getName());
        assertEquals ("artemis", nodes[1].getName());
        LOG.info("done!");
    }
}
