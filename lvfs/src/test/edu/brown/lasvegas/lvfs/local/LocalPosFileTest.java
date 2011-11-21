package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;

import org.junit.Test;

import edu.brown.lasvegas.lvfs.local.LocalPosFile.Pos;

/**
 * Testcase for {@link LocalPosFile}.
 */
public class LocalPosFileTest {
    @Test
    public void testAll() throws Exception {
        File file = new File("test/local/test.pos");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        file.delete();
        ArrayList<Long> tuples = new ArrayList<Long>();
        ArrayList<Long> positions = new ArrayList<Long>();

        final int ENTRY_COUNT = 1000;
        long curTup = 0L;
        long curPos = 12L;
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            tuples.add(curTup);
            positions.add(curPos);
            curTup += 100;
            curPos += 1024; // about 1 entry per 1KB
        }

        LocalPosFile.createPosFile(file, tuples, positions);
        assertEquals (tuples.size() * 2 * 8, file.length());
        
        LocalPosFile posFile = new LocalPosFile(file);
        Pos pos;

        pos = posFile.searchPosition(56052);
        assertEquals (56000, pos.tuple);
        assertEquals (12 + 1024 * 560, pos.bytePosition);

        pos = posFile.searchPosition(78489);
        assertEquals (78400, pos.tuple);
        assertEquals (12 + 1024 * 784, pos.bytePosition);

        pos = posFile.searchPosition(78400);
        assertEquals (78400, pos.tuple);
        assertEquals (12 + 1024 * 784, pos.bytePosition);

        pos = posFile.searchPosition(0);
        assertEquals (0, pos.tuple);
        assertEquals (12, pos.bytePosition);

        pos = posFile.searchPosition(100001);
        assertEquals (99900, pos.tuple);
        assertEquals (12 + 1024 * 999, pos.bytePosition);

        pos = posFile.searchPosition(99902);
        assertEquals (99900, pos.tuple);
        assertEquals (12 + 1024 * 999, pos.bytePosition);

        pos = posFile.searchPosition(99900);
        assertEquals (99900, pos.tuple);
        assertEquals (12 + 1024 * 999, pos.bytePosition);
    }
}
