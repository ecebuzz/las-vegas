package edu.brown.lasvegas.lvfs.meta;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Testcase for {@link MasterMetadataRepository}.
 */
public class MasterMetadataRepositoryTest extends MetadataRepositoryTest {
    private static final String TEST_BDB_HOME = "test/bdb_data";
    private static MasterMetadataRepository staticRepository;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        staticRepository = new MasterMetadataRepository(true, TEST_BDB_HOME);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        staticRepository.close();
        staticRepository = null;
    }

    @Before
    public void setUp() throws Exception {
        super.repository = staticRepository;
    }

    @After
    public void tearDown() throws Exception {
        super.repository = null;
    }
    
    @Override
    protected void reloadRepository() throws IOException {
        staticRepository.close();
        staticRepository = new MasterMetadataRepository(false, TEST_BDB_HOME);
        super.repository = staticRepository;
    }
}
