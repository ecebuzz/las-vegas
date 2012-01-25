package edu.brown.lasvegas.server;

import static org.junit.Assert.*;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Testcases to check the behavior of Configuration#addDefaultResource() for our conf xml.
 */
public class DefaultConfigTest {
    private static final String ABSOLUTE_PATH = "/edu/brown/lasvegas/server/lvfs_conf_template.xml";
    private static final String RELATIVE_PATH = "lvfs_conf_template.xml";
    @Test
    public void testDirect () throws Exception {
        {
            InputStream test = getClass().getResourceAsStream(ABSOLUTE_PATH);
            test.read();
            test.close();
        }
        {
            //ConfFileUtil is in the same package as the xml
            InputStream test = ConfFileUtil.class.getResourceAsStream(RELATIVE_PATH);
            test.read();
            test.close();
        }
    }
    @Test
    public void testAddResourceAbsolute () throws Exception {
        {
            Configuration conf = new Configuration();
            conf.addResource(getClass().getResource(ABSOLUTE_PATH));
            assertNotNull (conf.get(LVCentralNode.METAREPO_ADDRESS_KEY));
            assertNotNull (conf.get(LVCentralNode.METAREPO_BDBHOME_KEY));
        }
        {
            Configuration conf = new Configuration();
            conf.addResource(Configuration.class.getResource(ABSOLUTE_PATH));
            assertNotNull (conf.get(LVCentralNode.METAREPO_ADDRESS_KEY));
            assertNotNull (conf.get(LVCentralNode.METAREPO_BDBHOME_KEY));
        }
    }
    @Test
    public void testAddResourceRelative () throws Exception {
        {
            Configuration conf = new Configuration();
            //ConfFileUtil is in the same package as the xml
            conf.addResource(ConfFileUtil.class.getResource(RELATIVE_PATH));
            assertNotNull (conf.get(LVCentralNode.METAREPO_ADDRESS_KEY));
            assertNotNull (conf.get(LVCentralNode.METAREPO_BDBHOME_KEY));
        }
    }
    /*
    this testcase doesn't pass. So, commented out.
    Seems like addDefaultResource() only works for a file directly in the classpath!! so inconvenient.
    but I can't change Hadoop's behavior. so, I'm gonna move all conf files in the root...
    @Test
    public void testAddDefaultResource () throws Exception {
        {
            // Configuration.addDefaultResource(RELATIVE_PATH);
            Configuration.addDefaultResource(ABSOLUTE_PATH);
            Configuration conf = new Configuration();
            assertNotNull (conf.get(LVCentralNode.METAREPO_ADDRESS_KEY));
            assertNotNull (conf.get(LVCentralNode.METAREPO_BDBHOME_KEY));
        }
    }
    */
}
