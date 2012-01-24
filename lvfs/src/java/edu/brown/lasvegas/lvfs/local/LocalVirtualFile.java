package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * Implementation of VirtualFile that directly uses java.io.File.
 */
public final class LocalVirtualFile implements VirtualFile {
    private File file;
    public LocalVirtualFile (File file) {
        this.file = file;
    }
    public LocalVirtualFile (String path) {
        this.file = new File(path);
    }
    public LocalVirtualFile (String parent, String child) {
        this.file = new File(parent, child);
    }
    public LocalVirtualFile (File parent, String child) {
        this.file = new File(parent, child);
    }
    
    @Override
    public VirtualFileInputStream getInputStream() throws IOException {
        return new LocalVirtualFileInputStream(new FileInputStream(file));
    }
    @Override
    public VirtualFileOutputStream getOutputStream() throws IOException {
        return new LocalVirtualFileOutputStream(new FileOutputStream(file));
    }
    @Override
    public long length() {
        return file.length();
    }
    @Override
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }
    @Override
    public String getName() {
        return file.getName();
    }
    @Override
    public boolean delete() throws IOException {
        return delete (false);
    }
    @Override
    public boolean delete(boolean recursive) throws IOException {
        // TODO implement recursive delete. but, I'd be very hesitant to implement it. let's do this last.
        return file.delete();
    }
    @Override
    public boolean exists() {
        return file.exists();
    }
    @Override
    public boolean isDirectory() throws IOException {
        return file.isDirectory();
    }
    @Override
    public VirtualFile getParentFile() {
        File parentFile = file.getParentFile();
        if (parentFile == null) {
            return null;
        }
        return new LocalVirtualFile(parentFile);
    }
    @Override
    public VirtualFile getChildFile(String filename) {
        return new LocalVirtualFile(new File(file, filename));
    }
    @Override
    public boolean mkdirs() {
        return file.mkdirs();
    }
    @Override
    public boolean renameTo(VirtualFile newPath) throws IOException {
        assert (exists());
        File dest = ((LocalVirtualFile) newPath).file;
        if (dest.exists()) {
            boolean deleted = dest.delete();
            if (!deleted) {
                return false;
            }
        }
        File destParent = dest.getParentFile();
        if (!destParent.exists()) {
            boolean dirCreated = destParent.mkdirs();
            assert (dirCreated);
        }
        boolean success = file.renameTo(dest);
        if (success) {
            file = dest;
        }
        return success;
    }
    
    @Override
    public String toString() {
        return "LocalFile:" + getAbsolutePath();
    }
}
