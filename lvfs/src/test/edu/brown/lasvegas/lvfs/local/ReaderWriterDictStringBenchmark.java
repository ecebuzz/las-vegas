package edu.brown.lasvegas.lvfs.local;

import java.io.File;

/**
 * Tests the performance of bulk reading and writing for variable-length string with dictionary compression.
 * This is not a testcase.
 */
public class ReaderWriterDictStringBenchmark {
    public static void main(String[] args) throws Exception {
        File file = new File("test/local/string");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        File dictFile = new File("test/local/string.dict");
        File tmpFile = new File("test/local/string.tmp");
        String[] buf = new String[1 << 15]; // 32K * 16 = 500k bytes
        for (int j = 0; j < buf.length; ++j) {
            buf[j] = "string--" + String.format("%06d", j);
        }
        short[] readBuf = new short[1 << 15];
        {
            // JVM warm-up
            File dummy = new File("test/local/dummy");
            File dummyDict = new File("test/local/dummy.dict");
            File dummyTmp = new File("test/local/dummy.tmp");
            for (int rep = 0; rep < 3; ++rep) {
                {
                    System.currentTimeMillis();
                    LocalDictCompressionStringWriter writer = new LocalDictCompressionStringWriter(dummy, dummyDict, dummyTmp);
                    for (int i = 0; i < 20; ++i) {
                        writer.writeValues(buf, 0, buf.length);
                    }
                    writer.writeFileFooter();
                    writer.flush();
                    writer.close();
                }
                {
                    LocalDictFile dict = new LocalDictFile(dummyDict);
                    assert (dict.getBytesPerEntry() == 2);
                    assert (dict.getDictionary().length == 65536);
                }
                {
                    LocalFixLenReader<Short, short[]> reader = LocalFixLenReader.getInstanceSmallint(dummy);
                    for (int i = 0; i < 20; ++i) {
                        reader.readValues(readBuf, 0, readBuf.length);
                    }
                    reader.close();        
                }
            }
        }
        if (args.length == 0 || args[0].equalsIgnoreCase("writeonly") || args[0].equalsIgnoreCase("writeonly_sync")) {
            long startTime = System.currentTimeMillis();
            LocalDictCompressionStringWriter writer = new LocalDictCompressionStringWriter(file, dictFile, tmpFile);
            for (int i = 0; i < 512; ++i) {
                writer.writeValues(buf, 0, buf.length);
            }
            writer.writeFileFooter();
            writer.flush(args.length == 0 || args[0].equalsIgnoreCase("writeonly_sync"));
            writer.close();
            long endTime = System.currentTimeMillis();
            System.out.println("wrote " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }

        if (args.length == 0 || args[0].equalsIgnoreCase("readonly")) {
            long startTime = System.currentTimeMillis();
            LocalDictFile dict = new LocalDictFile(dictFile);
            assert (dict.getBytesPerEntry() == 2);
            assert (dict.getDictionary().length == 65536);
            LocalFixLenReader<Short, short[]> reader = LocalFixLenReader.getInstanceSmallint(file);
            for (int i = 0; i < 512; ++i) {
                reader.readValues(readBuf, 0, readBuf.length);
            }
            reader.close();        
            long endTime = System.currentTimeMillis();
            System.out.println("read " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }
    }
}
