package edu.brown.lasvegas.tuple;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Base64;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.CompressionType;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.data.PartitionedTextFileReader;
import edu.brown.lasvegas.util.ByteArray;

/**
 * A tuple reader implementation which reads one or more text files.
 * Each line must be separated by CR or LF. Each column must be separated by some delimiter (can be specified).
 * VARBINARY column must be BASE64 encoded.
 * 
 * This class also implements {@link #sample(TupleBuffer)} method, but in a very inefficient way.
 */
public class TextFileTupleReader extends DefaultTupleReader implements SampleableTupleReader {
    private final VirtualFile[] textFiles;
    private final CompressionType textFileCompression;

    private final int buffersize;
    private final Charset charset;
    private final String delimiter;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    private PartitionedTextFileReader reader;

    /** index in #textFiles. */
    private int nextTextFile = 0;
    private String currentLine;

    public TextFileTupleReader (VirtualFile textFile, ColumnType[] columnTypes, String delimiter) throws IOException {
        this (new VirtualFile[]{textFile}, columnTypes, delimiter);
    }
    public TextFileTupleReader (VirtualFile[] textFiles, ColumnType[] columnTypes, String delimiter) throws IOException {
        this (textFiles, CompressionType.NONE, columnTypes, delimiter, 1 << 10,
            Charset.forName("UTF-8"), new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }
    public TextFileTupleReader (
        VirtualFile[] textFiles, CompressionType textFileCompression,
        ColumnType[] columnTypes, String delimiter, int buffersize, Charset charset,
        DateFormat dateFormat, DateFormat timeFormat, DateFormat timestampFormat)
    throws IOException {
        super (columnTypes);
        this.textFiles = textFiles;
        this.textFileCompression = textFileCompression;
        this.delimiter = delimiter;
        this.buffersize = buffersize;
        this.charset = charset;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
    }
    @Override
    public String getCurrentTupleAsString() {
        return currentLine;
    }    

    private void closeCurrentFile () throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
    private boolean openNextFile () throws IOException {
        closeCurrentFile();
        if (nextTextFile >= textFiles.length) {
            return false; // no more files
        }
        reader = new PartitionedTextFileReader(textFiles[nextTextFile], charset, textFileCompression, buffersize);
        ++nextTextFile;
        return true;
    }

    @Override
    public void close() throws IOException {
        closeCurrentFile();
    }

    @Override
    public boolean next() throws IOException {
        String line;
        while (true) {
            if (reader == null) {
                boolean opened = openNextFile ();
                if (opened) {
                    continue;
                } else {
                    return false;
                }
            }
            line = reader.readLine();
            if (line == null) {
                boolean opened = openNextFile ();
                if (opened) {
                    continue;
                }
                Arrays.fill(currentData, null);
                return false;
            }
            if (line.length() == 0) {
                continue;
            }
            break;
        }
        parseLine (line, currentData);
        currentLine = line;
        return true;
    }
    private void parseLine(String line, Object[] dest) throws IOException {
        try {
            StringTokenizer tokenizer = new StringTokenizer(line, delimiter);
            for (int i = 0; i < columnCount; ++i) {
                String token = tokenizer.nextToken();
                if (columnTypes[i] == null) {
                    continue;
                }
                switch (columnTypes[i]) {
                case BIGINT: dest[i] = Long.valueOf(token); break;
                case DOUBLE: dest[i] = Double.valueOf(token); break;
                case FLOAT: dest[i] = Float.valueOf(token); break;
                case INTEGER: dest[i] = Integer.valueOf(token); break;
                case SMALLINT: dest[i] = Short.valueOf(token); break;
                case TINYINT: dest[i] = Byte.valueOf(token); break;
                case VARCHAR: dest[i] = token; break;
                case VARBINARY: dest[i] = new ByteArray(Base64.decodeBase64(token)); break;

                case BOOLEAN: dest[i] = Boolean.valueOf(token) ? (byte) 1 : (byte) 0; break;
                case DATE: dest[i] = dateFormat.parse(token).getTime(); break;
                case TIME: dest[i] = timeFormat.parse(token).getTime(); break;
                case TIMESTAMP: dest[i] = timestampFormat.parse(token).getTime(); break;
                }
            }
        } catch (Exception ex) {
            throw new IOException ("invalid line. failed to parse:" + line, ex);
        }
    }

    @Override
    public int sample(TupleBuffer buffer) throws IOException {
        nextTextFile = 0;
        closeCurrentFile();
        int sampleSize = buffer.getBufferSize();
        Object[][] samples = new Object[sampleSize][];
        int currentSamples = 0;
        Random random = new Random(12345L); // fixed seed for repeat-ability. should be able to specify by user 
        while (next()) {
            if (currentSamples < sampleSize) {
                samples[currentSamples] = currentData.clone();
                ++currentSamples;
            } else {
                // replace randomly
                int victim = random.nextInt(currentSamples);
                System.arraycopy(currentData, 0, samples[victim], 0, columnCount);
            }
        }
        DummyTupleReader dummyReader = new DummyTupleReader(columnTypes, samples, currentSamples);
        while (buffer.appendTuple(dummyReader));
        return currentSamples;
    }
}
