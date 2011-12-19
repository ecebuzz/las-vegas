package edu.brown.lasvegas.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.lang.NotImplementedException;

import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileInputStream;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * Dummy implementation of VirtualFile that proxies java.net.URL.
 * Obviously, this version supports only InputStream and lacks many features.
 * Use this only for some testcases! (e.g., to load resources from classloader)
 */
public final class URLVirtualFile implements VirtualFile {
    private final URL url;
    public URLVirtualFile (URL url) {
        this.url = url;
    }
    
    @Override
    public VirtualFileInputStream getInputStream() throws IOException {
        return new URLVirtualFileInputStream(url.openStream());
    }
    @Override
    public VirtualFileOutputStream getOutputStream() throws IOException {
        throw new NotImplementedException();
    }
    @Override
    public long length() throws IOException {
        URLConnection connection = url.openConnection();
        int len = connection.getContentLength();
        return len;
    }
    @Override
    public String getAbsolutePath() {
        return url.toExternalForm();
    }
    @Override
    public String getName() {
        return url.getFile();
    }
    @Override
    public boolean delete() throws IOException {
        throw new NotImplementedException();
    }
    @Override
    public boolean delete(boolean recursive) throws IOException {
        throw new NotImplementedException();
    }
    @Override
    public boolean exists() {
        throw new NotImplementedException();
    }
    @Override
    public boolean isDirectory() throws IOException {
        throw new NotImplementedException();
    }
    @Override
    public VirtualFile getParentFile() {
        throw new NotImplementedException();
    }
    @Override
    public boolean mkdirs() {
        throw new NotImplementedException();
    }
}
