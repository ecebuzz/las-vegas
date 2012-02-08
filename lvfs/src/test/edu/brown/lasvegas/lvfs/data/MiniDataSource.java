package edu.brown.lasvegas.lvfs.data;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.tuple.TextFileTupleReader;
import edu.brown.lasvegas.util.URLVirtualFile;

public abstract class MiniDataSource {
    public abstract ColumnType[] getScheme();
    public abstract URL getFileURL();

    public TextFileTupleReader open() throws IOException {
        URL testFile = getFileURL();
        ColumnType[] scheme = getScheme();
        TextFileTupleReader reader = new TextFileTupleReader(new VirtualFile[]{new URLVirtualFile(testFile)}, scheme, '|');
        return reader;
    }
    public byte[] getFileBody() throws IOException {
        URL testFile = getFileURL();
        InputStream in = testFile.openStream();
        byte[] bytes = new byte[in.available()];
        int read = in.read(bytes);
        assert (bytes.length == read);
        in.close();
        return bytes;
    }
    
    public abstract CompressionType[] getDefaultCompressions();
    public abstract String[] getColumnNames ();
    public abstract int getCount ();
}
