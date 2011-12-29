package edu.brown.lasvegas.lvfs.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.OrderedDictionary;
import edu.brown.lasvegas.lvfs.TypedDictWriter;
import edu.brown.lasvegas.lvfs.ValueTraits;
import edu.brown.lasvegas.lvfs.VirtualFile;

/**
 * A File writer for a dictionary-compressed column.
 * 
 * <p>This writer outputs 2 files, a compressed fixed-len integer file and
 * a dictionary file. The fixed-len integer file is the main data file
 * and one of 1-byte, 2-bytes, and 4-bytes compression scheme.</p>
 * 
 * <p>The writer internally uses one more temporary file, which always stores 4-bytes integers.
 * The values are first compressed to this file with a tentative non-order-preserving
 * dictionary, and then converted/compressed to the final file after the dictionary is fixed.
 * These final conversions are done in writeFileFooter() because it is called by the user
 * at the end of all writes.</p>
 */
public class LocalDictCompressionWriter<T extends Comparable<T>, AT> implements TypedDictWriter<T, AT> {
    private static Logger LOG = Logger.getLogger(LocalDictCompressionWriter.class);
 
    /** tentatively compressed file. */
    private final LocalFixLenWriter<Integer, int[]> tentativeIntWriter;
    private final int[] tentativeIntBuffer = new int[1 << 14];
    private int tentativeIntBufferUsed = 0;

    /**
     * Tentative non-order-preserving dictionary.
     * key=original string, value=compressed value (>=0) 
     */
    private final HashMap<T, Integer> tentativeDict;
    private final ArrayList<T> tentativeDictArray;
    
    private final VirtualFile finalDataFile;
    private final VirtualFile finalDictFile;
    private final VirtualFile tmpFile;

    private final ValueTraits<T, AT> traits;
    
    private OrderedDictionary<T, AT> finalDict;
    
    /**
     * @param finalDataFile the final compressed data file 
     * @param finalDictFile the final dictionary file
     * @param tmpFile Used to tentatively write out data. Deleted after used.
     * @param type value type BEFORE compression
     */
    @SuppressWarnings("unchecked")
    public LocalDictCompressionWriter(VirtualFile finalDataFile, VirtualFile finalDictFile, VirtualFile tmpFile, ColumnType type) throws IOException {
        this (finalDataFile, finalDictFile, tmpFile, (ValueTraits<T, AT>) AllValueTraits.getInstance(type));
    }

    public LocalDictCompressionWriter(VirtualFile finalDataFile, VirtualFile finalDictFile, VirtualFile tmpFile, ValueTraits<T,AT> traits) throws IOException {
        this.finalDataFile = finalDataFile;
        this.finalDictFile = finalDictFile;
        this.tmpFile = tmpFile;
        this.traits = traits;
        tentativeDict = new HashMap<T, Integer> (1 << 16, 0.25f); // for performance, use low load factor
        tentativeDictArray = new ArrayList<T> (1 << 16);
        tentativeIntWriter = LocalFixLenWriter.getInstanceInteger(tmpFile);
        
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
    }
    
    @Override
    public OrderedDictionary<T, AT> getFinalDict () {
        return finalDict;
    }

    @Override
    public void flush() throws IOException {}
    @Override
    public void flush(boolean sync) throws IOException {}
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
        AT dict;
        int[] dictConversion; // index=tentative dict's value, value=final dict's value
        byte bytesPerEntry;
        {
            long startMillisec = System.currentTimeMillis();
            int dictCount = tentativeDictArray.size();
            if (dictCount <= (1 << 8)) {
                bytesPerEntry = 1;
            } else if (dictCount <= (1 << 16)) {
                bytesPerEntry = 2;
            } else {
                bytesPerEntry = 4;
            }

            dict = traits.toArray(tentativeDictArray);
            int[] tentativePos = new int[dictCount];
            for (int i = 0; i < dictCount; ++i) {
                tentativePos[i] = i;
            }
            // now, sort the dict coupled with tentativePos. 
            traits.sortKeyValue(dict, tentativePos);
            // for example, suppose the tentative (unordered) dict was: D,A,C,B,E
            // before sorting, tentativePos was: 0,1,2,3,4. After sorting: 1,3,2,0,4
            // because the final data will be encoded as 'byte' from -128 to 127,
            //       tentative value=1 (A): -128 in final data
            //       tentative value=3 (B): -127 in final data
            //       tentative value=2 (C): -126 in final data
            //       tentative value=0 (D): -125 in final data
            //       tentative value=4 (E): -124 in final data
            // hence, dictConversion should be [-125,-128,-126,-127,-124]
            // we generate the mapping from tentativePos as follows
            dictConversion = new int[dictCount];
            for (int i = 0; i < dictCount; ++i) {
                int finalValue;
                switch (bytesPerEntry) {
                case 1:
                    finalValue = i - (1 << 7);
                    break;
                case 2:
                    finalValue = i - (1 << 15);
                    break;
                default:
                    assert (bytesPerEntry == 4);
                    finalValue = i ^ 0x80000000;
                    break;
                }
                dictConversion[tentativePos[i]] = finalValue;
            }

            long endMillisec = System.currentTimeMillis();
            if (LOG.isInfoEnabled()) {
                LOG.info("sorted the dictionary in " + (endMillisec - startMillisec) + "ms");
            }
        }
        finalDict = new LocalDictFile<T, AT>(dict, traits);
        finalDict.writeToFile(finalDictFile);

        // finally, convert the tentative integer file to the final integer file.
        LocalFixLenWriter<?, ?> finalDataWriter;
        long crc32Value;
        {
            long startMillisec = System.currentTimeMillis();
            LocalFixLenReader<Integer, int[]> tentativeReader = LocalFixLenReader.getInstanceInteger(tmpFile);
            switch (bytesPerEntry) {
            case 1:
            {
                LocalFixLenWriter<Byte, byte[]> finalByteWriter = LocalFixLenWriter.getInstanceTinyint(finalDataFile);
                finalDataWriter = finalByteWriter;
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
                break;
            }
            case 2:
            {
                LocalFixLenWriter<Short, short[]> finalShortWriter = LocalFixLenWriter.getInstanceSmallint(finalDataFile);
                finalDataWriter = finalShortWriter;
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
                break;
            }
            default:
            {
                assert(bytesPerEntry == 4);
                LocalFixLenWriter<Integer, int[]> finalIntWriter = LocalFixLenWriter.getInstanceInteger(finalDataFile);
                finalDataWriter = finalIntWriter;
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
                break;
            }
            }
            tentativeReader.close();
            finalDataWriter.writeFileFooter();
            finalDataWriter.flush();
            finalDataWriter.close();
            crc32Value = finalDataWriter.getRawValueWriter().getCRC32Value();
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
    public void writeValue(T value) throws IOException {
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
    public void writeValues(AT values, int off, int len) throws IOException {
        for (int i = off; i < off + len; ++i) {
            writeValue (traits.get(values, i));
        }
    }
    @Override
    public void setCRC32Enabled(boolean enabled) {
        crc32Enabled = enabled; // just keep the setting because the final writer will be constructed later
    }
    private boolean crc32Enabled = false;
}