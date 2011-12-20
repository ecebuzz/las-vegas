package edu.brown.lasvegas.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.LVColumn;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.protocol.MetadataProtocol;

/**
 * Testcases for {@link CentralNode}.
 */
public class CentralNodeTest {
    private static final String METAREPO_ADDRESS = "localhost:18711"; // use a port different from the default.
    private static final String METAREPO_BDBHOME = "test/metatest2";

    @Test
    public void testConnect () throws Exception {
        File bdbFolder = new File(METAREPO_BDBHOME);
        if (bdbFolder.exists()) {
            File backup = new File(bdbFolder.getParentFile(), bdbFolder.getName() + "_backup_"
                        + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) // append backup-date
                        + "_" + new Random(System.nanoTime()).nextInt()); // to make it unique
            boolean renamed = bdbFolder.renameTo(backup);
            if (!renamed) {
                throw new IOException ("failed to take a backup of existing testing-bdbhome");
            }
        }

        Configuration conf = new Configuration();
        conf.set(CentralNode.METAREPO_ADDRESS_KEY, METAREPO_ADDRESS);
        conf.set(CentralNode.METAREPO_BDBHOME_KEY, METAREPO_BDBHOME);
        CentralNode centralNode = CentralNode.createInstance(conf);
        
        MetadataProtocol metaClient = RPC.getProxy(MetadataProtocol.class, MetadataProtocol.versionID, NetUtils.createSocketAddr(METAREPO_ADDRESS), conf);

        LVColumn[] columns = new LVColumn[2];
        columns[0] = new LVColumn();
        columns[0].setFracturingColumn(false);
        columns[0].setName("col1");
        columns[0].setType(ColumnType.INTEGER);
        columns[1] = new LVColumn();
        columns[1].setFracturingColumn(false);
        columns[1].setName("col2");
        columns[1].setType(ColumnType.VARCHAR);
        LVTable table = metaClient.createNewTable("ttt", columns);
        assertTrue (table.getTableId() > 0);
        
        columns = metaClient.getAllColumns(table.getTableId());
        assertEquals (2 + 1, columns.length);
        assertEquals (LVColumn.EPOCH_COLUMN_NAME, columns[0].getName());
        assertEquals ("col1", columns[1].getName());
        assertEquals ("col2", columns[2].getName());

        centralNode.stop();
        centralNode.join();
    }
}
