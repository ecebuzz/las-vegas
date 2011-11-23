package edu.brown.lasvegas.lvfs.local;

import java.io.File;

/**
 * Tests the performance of bulk reading and writing.
 * This is not a testcase.
 */
public class ReaderWriterBenchmark {
    public static void main(String[] args) throws Exception {
        File file = new File("test/local/bench.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        short[] buf = new short[1 << 17];
        for (int j = 0; j < buf.length; ++j) {
            buf[j] = (short) j;
        }
        {
            // JVM warm-up
            File dummy = new File("test/local/dummy.bin");
            for (int rep = 0; rep < 3; ++rep) {
                {
                    System.currentTimeMillis();
                    LocalFixLenWriter<Short, short[]> writer = LocalFixLenWriter.getInstanceSmallint(dummy);
                    for (int i = 0; i < 20; ++i) {
                        writer.writeValues(buf, 0, buf.length);
                    }
                    writer.writeFileFooter();
                    writer.flush();
                    writer.close();
                }
                {
                    LocalFixLenReader<Short, short[]> reader = LocalFixLenReader.getInstanceSmallint(dummy);
                    for (int i = 0; i < 20; ++i) {
                        reader.readValues(buf, 0, buf.length);
                    }
                    reader.close();        
                }
            }
        }
        if (args.length == 0 || args[0].equalsIgnoreCase("writeonly") || args[0].equalsIgnoreCase("writeonly_sync")) {
            long startTime = System.currentTimeMillis();
            LocalFixLenWriter<Short, short[]> writer = LocalFixLenWriter.getInstanceSmallint(file);
            for (int i = 0; i < 1024; ++i) {
                writer.writeValues(buf, 0, buf.length);
            }
            writer.writeFileFooter();
            writer.flush(args[0].equalsIgnoreCase("writeonly_sync"));
            writer.close();
            long endTime = System.currentTimeMillis();
            System.out.println("wrote " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }

        if (args.length == 0 || args[0].equalsIgnoreCase("readonly")) {
            long startTime = System.currentTimeMillis();
            LocalFixLenReader<Short, short[]> reader = LocalFixLenReader.getInstanceSmallint(file);
            for (int i = 0; i < 1024; ++i) {
                reader.readValues(buf, 0, buf.length);
            }
            reader.close();        
            long endTime = System.currentTimeMillis();
            System.out.println("read " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }
    }
}
