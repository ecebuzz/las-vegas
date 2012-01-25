package edu.brown.lasvegas;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;

import org.junit.Test;

/**
 * Run this to delete all test data folder.
 * This testcase is named and placed such that it's executed first.
 */
public class ClearAllTest {
    @Test
    public void deleteAll () throws IOException {
        for (File file : new File("test").listFiles()) {
            assertTrue(deleteFileRecursive (file));
        }
    }
    public static boolean deleteFileRecursive (File dir) throws IOException {
        if (!dir.isDirectory()) {
            return dir.delete();
        }
        for (File child : dir.listFiles()) {
            if (!deleteFileRecursive(child)) {
                return false;
            }
        }
        return dir.delete();
    }
}
