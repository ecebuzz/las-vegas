package edu.brown.lasvegas.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * Utility methods related file ({@link VirtualFile}) manipulation.
 */
public final class VirtualFileUtil {
    private VirtualFileUtil () {}
    
    /**
     * Copies the contents of src file to the dest file.
     * <p>If the dest file already exists, this method overwrites the existing file.</p>
     * <p>If the folder containing dest file doesn't exist, this method creates it.</p>
     * <p>This method might internally exploit optimizations specific to implementation-type.
     * For example, if both src and dest are LocalVirtualFile,
     * we might use {@link FileChannel#transferFrom(java.nio.channels.ReadableByteChannel, long, long)}
     * for efficient copying.</p>
     */
    public static void copyFile (VirtualFile src, VirtualFile dest) throws IOException {
        assert (src != null);
        assert (dest != null);
        copyFile (src, dest, 1 << 20);
    }
    /** overload with the given read/write buffer size. */
    public static void copyFile (VirtualFile src, VirtualFile dest, int bufferSize) throws IOException {
        // having said that, the optimizations are future work..
        // simply copy.
        assert (src != null);
        assert (dest != null);
        VirtualFile parentFolder = dest.getParentFile();
        if (!parentFolder.exists()) {
            parentFolder.mkdirs();
            if (!parentFolder.exists()) {
                throw new IOException("failed to create a directory: " + parentFolder);
            }
        }
        byte[] buffer = new byte[bufferSize];
        OutputStream out = dest.getOutputStream();
        try {
            InputStream in = src.getInputStream();
            try {
                while (true) {
                    int read = in.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    out.write(buffer, 0, read);
                }
            } finally {
                in.close();
            }
            out.flush();
        } finally {
            out.close();
        }
    }
}
