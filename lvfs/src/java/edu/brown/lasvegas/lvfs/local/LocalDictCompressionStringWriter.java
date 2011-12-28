package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.TypedWriter;
import edu.brown.lasvegas.lvfs.VirtualFile;
import edu.brown.lasvegas.lvfs.VirtualFileOutputStream;

/**
 * A special File writer for a dictionary-compressed
 * string column.
 * 
 * <p>This writer outputs 2 files, a compressed fixed-len integer file and
 * a dictionary file. The fixed-len integer file is the main data file
 * and one of 1-byte, 2-bytes, and 4-bytes compression scheme.</p>
 * 
 * <p>The writer internally uses one more temporary file, which always stores 4-bytes integers.
 * The string values are first compressed to this file with a tentative non-order-preserving
 * dictionary, and then converted/compressed to the final file after the dictionary is fixed.
 * These final conversions are done in writeFileFooter() because it is called by the user
 * at the end.</p>
 */
public class LocalDictCompressionStringWriter implements TypedWriter<String, String[]> {
    private static Logger LOG = Logger.getLogger(LocalDictCompressionStringWriter.class);
 
    /** final data file. one of followings. */
    private LocalFixLenWriter<Byte, byte[]> finalByteWriter =  null;
    private LocalFixLenWriter<Short, short[]> finalShortWriter =  null;
    private LocalFixLenWriter<Integer, int[]> finalIntWriter =  null;
    
    /** final dict file. */
    private final VirtualFileOutputStream finalDictWriter;
    
    /** tentatively compressed file. */
    private final LocalFixLenWriter<Integer, int[]> tentativeIntWriter;
    private final int[] tentativeIntBuffer = new int[1 << 14];
    private int tentativeIntBufferUsed = 0;

    /**
     * Tentative non-order-preserving dictionary.
     * key=original string, value=compressed value (>=0) 
     */
    private final HashMap<String, Integer> tentativeDict;
    private final ArrayList<String> tentativeDictArray;
    
    private final VirtualFile finalDataFile;
    private final VirtualFile finalDictFile;
    private final File tmpFile;
    
    /**
     * @param finalDataFile the final compressed data file 
     * @param finalDictFile the final dictionary file
     * @param tmpFile Used to tentatively write out data. Deleted after used.
     */
    public LocalDictCompressionStringWriter(VirtualFile finalDataFile, VirtualFile finalDictFile, File tmpFile) throws IOException {
        this.finalDataFile = finalDataFile;
        this.finalDictFile = finalDictFile;
        this.tmpFile = tmpFile;
        tentativeDict = new HashMap<String, Integer> (1 << 16, 0.25f); // for performance, use low load factor
        tentativeDictArray = new ArrayList<String> (1 << 16);
        tentativeIntWriter = LocalFixLenWriter.getInstanceInteger(new LocalVirtualFile(tmpFile));
        finalDictWriter = finalDictFile.getOutputStream();
        
        if (LOG.isInfoEnabled()) {
            LOG.info("Creating dict-comp string file. finalDataFile=" + finalDataFile.getAbsolutePath()
                            + ", finalDictFile=" + finalDictFile.getAbsolutePath()
                            + ", tmpFile=" + tmpFile.getAbsolutePath());
        }
    }
    
    @Override
    public void close() throws IOException {
        if (!wroteFileFooter) {
            // just warn. the user might have simply canceled writing this file 
            LOG.warn("this file format needs to finalize the compressed data, but close() was called before writeFileFooter().");
        }
        if (finalByteWriter != null) {
            finalByteWriter.close();
        }
        if (finalShortWriter != null) {
            finalShortWriter.close();
        }
        if (finalIntWriter != null) {
            finalIntWriter.close();
        }
        finalDictWriter.close();
    }

    @Override
    public void flush() throws IOException {
        flush (false);
    }
    @Override
    public void flush(boolean sync) throws IOException {
        if (finalByteWriter != null) {
            finalByteWriter.flush(sync);
        }
        if (finalShortWriter != null) {
            finalShortWriter.flush(sync);
        }
        if (finalIntWriter != null) {
            finalIntWriter.flush(sync);
        }
        finalDictWriter.flush();
        if (sync) {
            finalDictWriter.syncDurable(); // this really ensures the written data is durable.
        }
    }
    private boolean wroteFileFooter = false;
    @Override
    public long writeFileFooter() throws IOException {
        LOG.info ("writing final files...");
        // first, finish writing to tentative file
        if (tentativeIntBufferUsed > 0) {
            tentativeIntWriter.writeValues(tentativeIntBuffer, 0, tentativeIntBufferUsed);
        }
        // we only call flush(), not sync. we don't have to make sure it's durable. it's a tentative file. 
        tentativeIntWriter.flush();
        tentativeIntWriter.close();
        
        // next, create an order-preserving final dictionary
        if (LOG.isInfoEnabled()) {
            LOG.info ("writing final dictionary of " + tentativeDictArray.size() + " entries...");
        }
        String[] dict;
        int[] dictConversion; // index=tentative dict's value, value=final dict's value
        byte bytesPerEntry;
        {
            long startMillisec = System.currentTimeMillis();
            dictConversion = new int[tentativeDictArray.size()];
            Arrays.fill(dictConversion, Integer.MIN_VALUE); // just for debugging. not required.
            dict = tentativeDict.keySet().toArray(new String[0]);
            Arrays.sort(dict);
            if (dict.length <= (1 << 8)) {
                bytesPerEntry = 1;
            } else if (dict.length <= (1 << 16)) {
                bytesPerEntry = 2;
            } else {
                bytesPerEntry = 4;
            }

            // also create conversion mapping
            for (int i = 0; i < dict.length; ++i) {
                assert(i == dict.length - 1 || !dict[i].equals(dict[i + 1]));
                int tentativeValue = tentativeDict.get(dict[i]);
                int finalizedValue;
                switch (bytesPerEntry) {
                case 1: finalizedValue = i - (1 << 7); break;
                case 2: finalizedValue = i - (1 << 15); break;
                default: assert (bytesPerEntry == 4); finalizedValue = i ^ 0x80000000; break;
                }
                dictConversion[tentativeValue] = finalizedValue;
            }

            long endMillisec = System.currentTimeMillis();
            if (LOG.isInfoEnabled()) {
                LOG.info("sorted the dictionary in " + (endMillisec - startMillisec) + "ms");
            }
        }
        {
            long startMillisec = System.currentTimeMillis();
            // output into the final dict file
            ObjectOutputStream out = new ObjectOutputStream(finalDictWriter);
            out.writeObject(dict);
            out.flush();
            long endMillisec = System.currentTimeMillis();
            if (LOG.isInfoEnabled()) {
                LOG.info("Wrote out a dict file:" + dict.length + " entries, "
                            + finalDictFile.length() + " bytes, in " + (endMillisec - startMillisec) + "ms");
            }
        }

        // finally, convert the tentative integer file to the final integer file.
        long crc32Value;
        {
            long startMillisec = System.currentTimeMillis();
            LocalFixLenReader<Integer, int[]> tentativeReader = LocalFixLenReader.getInstanceInteger(new LocalVirtualFile(tmpFile));
            switch (bytesPerEntry) {
            case 1:
            {
                finalByteWriter = LocalFixLenWriter.getInstanceTinyint(finalDataFile);
                finalByteWriter.getRawValueWriter().setCRC32Enabled(crc32Enabled);
                byte[] buf = new byte[tentativeIntBuffer.length];
                while (true) {
                    int read = tentativeReader.readValues(tentativeIntBuffer, 0, tentativeIntBuffer.length);
                    if (read <= 0) break;
                    for (int i = 0; i < read; ++i) {
                        buf[i] = (byte) dictConversion[tentativeIntBuffer[i]];
                    }
                    finalByteWriter.writeValues(buf, 0, read);
                }
                crc32Value = finalByteWriter.getRawValueWriter().getCRC32Value();
                break;
            }
            case 2:
            {
                finalShortWriter = LocalFixLenWriter.getInstanceSmallint(finalDataFile);
                finalShortWriter.getRawValueWriter().setCRC32Enabled(crc32Enabled);
                short[] buf = new short[tentativeIntBuffer.length];
                while (true) {
                    int read = tentativeReader.readValues(tentativeIntBuffer, 0, tentativeIntBuffer.length);
                    if (read <= 0) break;
                    for (int i = 0; i < read; ++i) {
                        buf[i] = (short) dictConversion[tentativeIntBuffer[i]];
                    }
                    finalShortWriter.writeValues(buf, 0, read);
                }
                crc32Value = finalShortWriter.getRawValueWriter().getCRC32Value();
                break;
            }
            default:
            {
                assert(bytesPerEntry == 4);
                finalIntWriter = LocalFixLenWriter.getInstanceInteger(finalDataFile);
                finalIntWriter.getRawValueWriter().setCRC32Enabled(crc32Enabled);
                int[] buf = new int[tentativeIntBuffer.length];
                while (true) {
                    int read = tentativeReader.readValues(tentativeIntBuffer, 0, tentativeIntBuffer.length);
                    if (read <= 0) break;
                    for (int i = 0; i < read; ++i) {
                        buf[i] = dictConversion[tentativeIntBuffer[i]];
                    }
                    finalIntWriter.writeValues(buf, 0, read);
                }
                crc32Value = finalIntWriter.getRawValueWriter().getCRC32Value();
                break;
            }
            }
            tentativeReader.close();
            tmpFile.delete(); // no longer needed
            long endMillisec = System.currentTimeMillis();
            if (LOG.isInfoEnabled()) {
                LOG.info("finalized the data file in " + (endMillisec - startMillisec) + "ms");
            }
        }

        wroteFileFooter = true;
        return crc32Value;
    }
    @Override
    public void writeValue(String value) throws IOException {
        Integer compressed = tentativeDict.get(value);
        int compressedInt;
        if (compressed == null) {
            // new entry
            compressedInt = tentativeDictArray.size();
            tentativeDict.put(value, compressedInt);
            tentativeDictArray.add(value);
        } else {
            compressedInt = compressed.intValue();
        }
        // use writeValues to speed-up writing
        if (tentativeIntBufferUsed == tentativeIntBuffer.length) {
            tentativeIntWriter.writeValues(tentativeIntBuffer, 0, tentativeIntBuffer.length);
            tentativeIntBufferUsed = 0;
        }
        tentativeIntBuffer[tentativeIntBufferUsed] = compressedInt;
        ++tentativeIntBufferUsed;
    }
    @Override
    public void writeValues(String[] values, int off, int len) throws IOException {
        for (int i = off; i < off + len; ++i) {
            writeValue (values[i]);
        }
    }
    @Override
    public void setCRC32Enabled(boolean enabled) {
        crc32Enabled = enabled; // just keep the setting because the final writer will be constructed later
    }
    private boolean crc32Enabled = false;
}
