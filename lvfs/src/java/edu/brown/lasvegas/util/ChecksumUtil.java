package edu.brown.lasvegas.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Utility class for check sum calculation.
 */
public final class ChecksumUtil {
    private ChecksumUtil() {}
    
    /**
     * Returns the CRC32 value of given file.
     */
    public static long getFileCheckSum (VirtualFile file) throws IOException {
        InputStream in = file.getInputStream();
        byte[] buffer = new byte[1 << 16];
        CRC32 checksum = new CRC32();
        while (true) {
            int read = in.read(buffer);
            if (read == -1) {
                break;
            }
            checksum.update(buffer, 0, read);
        }
        in.close();
        return checksum.getValue();
    }
}
