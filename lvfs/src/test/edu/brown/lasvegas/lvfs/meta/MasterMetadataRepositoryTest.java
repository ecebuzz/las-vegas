package edu.brown.lasvegas.lvfs.meta;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Testcase for {@link MasterMetadataRepository}.
 */
public class MasterMetadataRepositoryTest extends MetadataRepositoryTestBase {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static MasterMetadataRepository masterRepository;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        masterRepository = new MasterMetadataRepository(true, TEST_BDB_HOME); // nuke the folder
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        masterRepository.shutdown();
        masterRepository = null;
    }

    @Before
    public void setUp() throws Exception {
        super.baseSetUp(masterRepository);
    }

    @After
    public void tearDown() throws Exception {
        super.baseTearDown();
    }
    
    @Override
    protected void reloadRepository() throws IOException {
        masterRepository.shutdown();
        masterRepository = new MasterMetadataRepository(false, TEST_BDB_HOME);
        repository = masterRepository;
    }
}
